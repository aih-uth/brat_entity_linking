import os
import re
import gc
import pandas as pd
import numpy as np
import mojimoji
import MeCab
from googletrans import Translator
from collections import defaultdict
from pkg_resources import parse_version
import sklearn
if parse_version(sklearn.__version__) > parse_version('0.23'):
    from sklearn.feature_extraction import _stop_words as stop_words
else:
    from sklearn.feature_extraction import stop_words
import normdb
import simstring_cpp
from simstring.feature_extractor.character_ngram import CharacterNgramFeatureExtractor
from simstring.measure.cosine import CosineMeasure
import sys
import sqlite3 as sqlite
import argparse
from message import Messager
import glob


stop_words_sklearn = stop_words.ENGLISH_STOP_WORDS

# 検索用 DB の初期化を行うか否かのフラグ
INIT_DB = False

UMLS_DB_NAME = 'UMLS.ss.db'
# UMLS_DB_PATH = '../../../work'
UMLS_DB_PATH = 'resource'

# simstring の設定
#NGRAM = 3
#SEARCH_THRESHOLD = 0.85
NGRAM = 2
SEARCH_THRESHOLD = 0.65


# 英数字判定
alnum = re.compile(r'^[a-zA-Z0-9\s\-\_]+$')
def is_alnum(string):
    return alnum.match(string) is not None

# 半角カナ判定(UTF-8)
harf = re.compile(r'^([ｦ-ﾟ]|[a-zA-Z0-9\s\-\_])+$')
def is_harf(string):
    return harf.match(string) is not None

TEST = False
if TEST:
    flag = is_alnum('ABdc　123-123')
    assert flag is True, 'is_alnum 判定ミス'
    flag = is_alnum('ｹｯｾｲLDHｿﾞｳｶ')
    assert flag is False, 'is_alnum 判定ミス'

    flag = is_harf('ｹｯｾｲｿﾞｳｶ')
    assert flag is True, 'is_harf 判定ミス'
    flag = is_harf('ｹｯｾｲLDHｿﾞｳｶ 123_456')
    assert flag is True, 'is_harf 判定ミス'


class UmlsSearcherCpp(object):
    def __init__(self, db_name, db, feature_extractor, measure):
        self.db_name = db_name
        self.db = db
        self.feature_extractor = feature_extractor
        self.measure = measure
        self.translator = Translator(service_urls=['translate.googleapis.com'])
        self.resource_path = os.path.join(os.path.dirname(__file__), 'resource')
        self.synonyms_db = os.path.join(self.resource_path, 'umls_synonyms.db')

    def ranked_search(self, query_string):
        """
        simstring の結果を score 付きで返す
        :param query_string:
        :return: (score, cui, synonym, SemanticType)
        """
        strs = self.db.retrieve(query_string)
        id_names = self.ids_by_names(strs)
        # reature は cui, synonym, SemanticType, representative の順で並ぶ
        features = self.feature_extractor.features(query_string)
        # result_with_score は score, cui, synonym, SemanticType の順で並ぶ
        results_with_score = list(map(lambda x: [self.measure.similarity(features,
                                                                         self.feature_extractor.features(x[1])), x[0], x[1], x[2], x[3], x[4]],
                                      id_names))
        # score の小さなものから順にソートする(後で CUI 単位に集約するときに最大 score が残るようにする）
        return sorted(results_with_score, key=lambda x: (x[0], x[1]))

    def ids_by_names(self, strs):
        connection, cursor = self._get_connection_cursor()

        command = 'SELECT cui, synonym, semantic, representative, in_use from umls_synonyms where synonym in (%s)' % ','.join(['?' for n in strs])
        cursor.execute(command, strs)
        response = cursor.fetchall()

        cursor.close()

        return response

    def _get_connection_cursor(self):
        # helper for DB access functions
        # open DB
        if not os.path.exists(self.synonyms_db):
            raise normdb.dbNotFoundError(self.synonyms_db)
        connection = sqlite.connect(self.synonyms_db)
        cursor = connection.cursor()

        return connection, cursor

DROP_COMMANDS = [
    'DROP TABLE IF EXISTS umls_synonyms;',
    'DROP INDEX IF EXISTS cui_idx;',
    'DROP INDEX IF EXISTS synonym_idx;',
]

CREATE_TABLE_COMMANDS = [
    """CREATE TABLE umls_synonyms (
  id INTEGER PRIMARY KEY,
  cui VARCHAR(8),
  semantic VARCHAR(255),
  synonym VARCHAR(255),
  representative VARCHAR(255),
  in_use INTEGER
);""",
]

CREATE_INDEX_COMMANDS = [
    "CREATE INDEX cui_idx ON umls_synonyms (cui);",
    "CREATE INDEX synonym_idx ON umls_synonyms (synonym);",
]


def init_db_cpp():

    resource_path = (os.path.join(os.path.dirname(__file__), UMLS_DB_PATH))
    df1 = pd.read_csv(os.path.join(resource_path, 'UMLS_synonyms.txt'), sep='\t')
    df1.fillna({'in_use': 0.0}, inplace=True)
    df1['in_use'].astype('int')
    df1 = df1.dropna()

    # Create a SimString database
    db_path_base = os.path.join(resource_path)
    if not os.path.exists(db_path_base):
        os.makedirs(db_path_base)
    simastring_db_path = os.path.join(db_path_base, UMLS_DB_NAME)
    if os.path.exists(simastring_db_path):
        os.remove(simastring_db_path)
        d_l = glob.glob(simastring_db_path + '.*.cdb')
        for d in d_l:
            os.remove(d)
    db = simstring_cpp.writer(simastring_db_path,
                              NGRAM, False, True)
    t_size = len(df1)

    # create SQL DB
    sqldbfn = os.path.join(db_path_base, 'umls_synonyms.db')
    try:
        connection = sqlite.connect(sqldbfn, isolation_level='EXCLUSIVE')
    except sqlite.OperationalError as e:
        print("Error connecting to DB %s:" % sqldbfn, e, file=sys.stderr)
        return 1
    cursor = connection.cursor()

    for drp in DROP_COMMANDS:
        try:
            cursor.execute(drp)
        except sqlite.OperationalError as e:
            print("Error dtop %s:" % drp, e, "(DB/INDEX exists?)", file=sys.stderr)
            return 1

    for command in CREATE_TABLE_COMMANDS:
        try:
            cursor.execute(command)
        except sqlite.OperationalError as e:
            print("Error creating %s:" % sqldbfn, e, "(DB exists?)", file=sys.stderr)
            return 1

    error_count = 0
    MAX_ERROR_LINES = 100
    count = 0
    prev_cui = ''
    representative = ''
    for cui, s_type, synonym, representative, in_use in zip(df1["cui"], df1["SemanticType"], df1["synonym"],
                                                          df1["representative"], df1["in_use"]):
        db.insert(synonym)

        # insert entity
        if prev_cui != cui:
            prev_cui = cui
            representative = synonym
        try:
            cursor.execute(
                "INSERT into umls_synonyms VALUES (?, ?, ?, ?, ?, ?)",
                (count, cui, s_type, synonym, representative, in_use))
        except sqlite.IntegrityError as e:
            if error_count < MAX_ERROR_LINES:
                print("Error inserting %s (skipping): %s" % (cui, e), file=sys.stderr)
            elif error_count == MAX_ERROR_LINES:
                print("(Too many errors; suppressing further error messages)", file=sys.stderr)
            error_count += 1
            continue

        count += 1
        if count % 100000 == 0:
            print('{}/{}'.format(count, t_size))
            connection.commit()

    connection.commit()
    for command in CREATE_INDEX_COMMANDS:
        try:
            cursor.execute(command)
        except sqlite.OperationalError as e:
            print("DB Error creating index: ", e, file=sys.stderr)
            raise e
    connection.commit()
    db.close()


def main():
    # '''
    # 初回のみ init_db() を実行して Mongo DB にデータを insert しておく
    if args.init_db:
        init_db_cpp()
    else:
        # '''
        querys_list = [["白血球数 15.0 ×千/μl"], ["身長"], ["身長", "body height"], ["身長 190 cm"], ["高い身長"],
                       ["白血球数 異常"], ["white blood cell disorder"], ["左大腿骨 頸部 骨折"],
                       ['血圧 200.8 mmHg'], ['血圧 150.4 mmHg', '血圧 100 mmHg'],
                       ["Standing Body Height"], ["ほげほげ"]]
        # '''
        # query_list[['身長'], ['高さ']]
        # querys_list = [["white blood cell disorder"], ["左大腿骨 頸部 骨折"]]
        searcher = load_dct()
        test_value_df = test_value_set()
        for querys in querys_list:
            querys = lab_value_normalization(querys, test_value_df)
            scored_concept = word2UMLS(querys,  searcher, database='UMLS')
            print(scored_concept)
        # '''


# 翻訳
def translate_Google(querys):
    translator = Translator()
    tmp = []
    for name in (querys):
        tmp.append(translator.translate(name).text)
    return tmp


def word2UMLS(querys, searcher, database):
    scored_concept = {}
    for query in querys:
        query = query.lower()
        is_alnum_flag = is_alnum(query)
        scored_concept, direct_hit = _word2umls_impl(query, scored_concept, is_alnum_flag, searcher, database)
        # if not direct_hit:
        if True:
            try:  # direct_hit に関係せず英語翻訳を実行するように変更してみた。(20211222)
                # googletrans はたまに失敗する。失敗したら googletrans を作り直して再検索する。
                # 5回失敗したら変換検索は諦める
                t_query = None
                for i in range(5):
                    try:
                        t_query = searcher.translator.translate(query, src='ja')
                        break
                    except Exception as e:
                        if i == 4:
                            # googletrans がどうしても成功しないとき
                            Messager.error(e)
                        searcher.translator = Translator(service_urls=['translate.googleapis.com'])
                if t_query is not None:
                    t_query = t_query.text.lower().replace('_', '')
                    scored_concept, direct_hit = _word2umls_impl(t_query, scored_concept, True, searcher, database)
            except ConnectionError as e:
                Messager.error(e)
    # UNKを先頭に
    if "C0439673" not in scored_concept:
        scored_concept["C0439673"] = (5.0, 'unknown', 'Qualitative Concept', 'Unknown', 0)  # 1.0 より大きい数字
    scored_concept = sorted(scored_concept.items(), key=lambda x: -1.0 * x[1][0])
    return scored_concept


def _search_id(searcher, query, alpha, database='UMLS'):
    results = searcher.ranked_search(query)
    # strs = [s[1] for s in results]
    # ersults は score の小さなものから並んでいるので順に辞書に積むと、最大の値が残る
    # results は score, cui, synonym, SemanticType の順で並ぶ
    norm_score = {cui: (score, synonym, type, representative, in_use) for (score, cui, synonym, type, representative, in_use) in results}

    return norm_score


def _concept_update(concept, results):
    """コンセプト辞書をマージする。重複したコンセプトに対しては最大スコアを割り当てる"""
    for k, (score, synonym, ty, rep, in_use) in results.items():
        if k not in concept:
            concept[k] = (score, synonym, ty, rep, in_use)
        else:
            # 新しい検索結果のスコアが大きい時だけスコアを入れ替える。
            if concept[k][0] < score:
                concept[k] = (score, synonym, ty, rep, in_use)


def _word2umls_impl(query, scored_concept, is_alnum_flag, searcher, database):
    # alpha が低いと検索がいちじるしく遅くなる
    # results = _search_id(searcher, query, 0.75, database)
    if not is_alnum_flag:
        base_score = 2.0
    else:
        base_score = 0.0
    org_len_features = float(len(searcher.feature_extractor.features(query)))
    results = _search_id(searcher, query.replace('_', ' '), SEARCH_THRESHOLD, database)
    if len(results) != 0:
        # 日本語検索は + 2点、ダイレクトヒットは +1 点にしておく
        results = {cui: (score + 1.0 + base_score, synonym, ty, rep, in_use) for cui, (score, synonym, ty, rep, in_use) in results.items()}
        _concept_update(scored_concept, results)
        direct_hit = True
    # ここでまだない場合は部分一致で検索を実行
    else:
        direct_hit = False
        # 形態素解析
        wakati = []
        if not is_alnum_flag:
            sep = ''
            mecab = MeCab.Tagger()
            for w in mecab.parse(query).split('\n'):
                if w == "EOS":
                    break
                else:
                    if w.split("\t")[1].split(",")[0] in ["名詞", "動詞", "形容詞"]:
                        # high とか low とかの文字列が hit するのを回避する。
                        # 英語検索の時に検索されるので、ここで無視しても問題ない。
                        if not is_alnum(w.split("\t")[0]):
                            wakati.append(w.split("\t")[0])
                    else:
                        pass
        else:
            sep = ' '
            wakati = [w.lower() for w in query.split(' ') if not w.lower() in stop_words_sklearn]

        # 右から1単語ずつ削る
        for i in range(len(wakati), 0, -1):
            partial_query = sep.join(wakati[:i])
            results = partial_search(searcher, partial_query, database, org_len_features, base_score)
            if len(results) != 0:
                _concept_update(scored_concept, results)
                break
        # 左側から1単語ずつ削る
        for i in range(1, len(wakati), 1):
            partial_query = sep.join(wakati[i:])
            results = partial_search(searcher, partial_query, database, org_len_features, base_score)
            if len(results) != 0:
                _concept_update(scored_concept, results)
                break
    return scored_concept, direct_hit


def partial_search(searcher, partial_query, database, org_len_features, base_score):
    results = _search_id(searcher, partial_query,
                         SEARCH_THRESHOLD, database)
    if len(results) != 0:
        feature_len = float(len(searcher.feature_extractor.features(partial_query)))
        # 文字を削って見つけた結果は減点する
        results = {cui: (score * feature_len / org_len_features + base_score, synonym, ty, rep, in_use) for
                   cui, (score, synonym, ty, rep, in_use) in results.items()}
    return results


def convert2df(sub, file):
    list_df = []
    for i in range(0, sub.shape[0], 1):
        tmp_df = sub.iloc[i]
        # 日本語の処理
        if pd.isnull(tmp_df["一致方法"]):
            pass
        else:
            words, methods, querys, reps, concepts =\
                tmp_df["単語"], tmp_df["一致方法"], tmp_df["クエリ"],\
                tmp_df["代表表記"], tmp_df["コンセプト"]
            # 代表表記は欠損の場合がある
            if pd.isnull(reps):
                reps = "None"
            else:
                pass
            tmp1, tmp2, tmp3, tmp4, tmp5, tmp6 =\
                [], [], [], [], [], []
            for y, z, k in zip(querys.split(","),
                               reps.split(","), concepts.split(",")):
                tmp1.append("JPN")
                tmp2.append(words)
                tmp3.append(methods)
                tmp4.append(y)
                tmp5.append(z)
                tmp6.append(k)
            sub1 = pd.DataFrame({"単語": tmp2, "一致方法": tmp3,
                                 "クエリ": tmp4, "代表表記": tmp5,
                                 "コンセプト": tmp6})
            sub1["no"] = i
            sub1["file"] = file
            list_df.append(sub1)
        # 英語の処理
        if pd.isnull(tmp_df["一致方法_eng"]):
            pass
        else:
            words_eng, methods, querys, reps, concepts =\
                tmp_df["単語_eng"], tmp_df["一致方法_eng"], tmp_df["クエリ_eng"],\
                tmp_df["代表表記_eng"], tmp_df["コンセプト_eng"]
            # 代表表記は欠損の場合がある
            if pd.isnull(reps):
                reps = "None"
            else:
                pass
            tmp1, tmp2, tmp3, tmp4, tmp5, tmp6 = [], [], [], [], [], []
            for y, z, k in zip(querys.split(","),
                               reps.split(","), concepts.split(",")):
                tmp1.append("ENG")
                tmp2.append("{1} ({0})".format(tmp_df["単語"], words_eng))
                tmp3.append(methods)
                tmp4.append(y)
                tmp5.append(z)
                tmp6.append(k)
            sub2 = pd.DataFrame({"単語": tmp2, "一致方法": tmp3,
                                 "クエリ": tmp4, "代表表記": tmp5, "コンセプト": tmp6})
            sub2["no"] = i
            sub2["file"] = file
            list_df.append(sub2)
    pd.concat(list_df).\
        to_csv("../{0}.csv".format(file.split(".")[0]), index=False)


def load_dct():
    # simstring
    db_path = (os.path.join(os.path.dirname(__file__), UMLS_DB_PATH))
    simstring_db = simstring_cpp.reader(os.path.join(db_path, UMLS_DB_NAME))
    simstring_db.measure = simstring_cpp.cosine
    simstring_db.threshold = SEARCH_THRESHOLD
    searcher = UmlsSearcherCpp('UMLS', simstring_db,
                               CharacterNgramFeatureExtractor(NGRAM),
                               CosineMeasure())
    gc.collect()
    return searcher


def test_value_set():
    resource_path = (os.path.join(os.path.dirname(__file__), 'resource'))
    test_df = pd.read_csv(os.path.join(resource_path, "test_value.csv"))
    names, upper, lower = [], [], []
    for x, y, z in zip(test_df["LOCAL_NAME"], test_df["上限"], test_df["下限"]):
        x = x.split("_")[0]
        for xx in re.findall("（.+?）", x):
            names.append(x.replace(xx, "").lower())
            upper.append(y)
            lower.append(z)
            names.append(xx.replace("（", "").replace("）", "").lower())
            upper.append(y)
            lower.append(z)
    # 追加
    names.extend([x.lower() for x in ["Creatine Kinase"]])
    upper.extend([210])
    lower.extend([50])
    # to DF
    df = pd.DataFrame({"name": names, "上限": upper, "下限": lower})
    df["name"] = [mojimoji.han_to_zen(x.replace(" ", "_")) for x in df["name"]]
    # 結合
    test_df = test_df[["LOCAL_NAME", "上限", "下限"]].rename(columns={'LOCAL_NAME': "name"})
    test_df = test_df[~test_df.duplicated()]
    return pd.concat([df, test_df])


def lab_value_normalization(querys, test_value_df):
    """
    queryの第一項目が検査を表す文字列であり、第二項目が数値の場合に、
    その値を検査値の標準範囲と比較して、標準範囲を超えている場合には 検査項目に"_high"、標準範囲を下回る場合は検査項目に"_low"を付与する。
    querys の順番は保証されないが、querys は基本的に並列関係になっているものと想定しているので順番が変更されても問題がないと考える。
    :param querys:
    :param test_value_df:
    :return:
    """
    norm_querys = []

    querys = [q.lower().replace("\u3000", " ") for q in querys]

    # 1) 合成されていない query には何もしない
    simple_querys = [q for q in querys if len(q.split(' ')) < 2]
    for query in simple_querys:
        norm_querys.append(query)
    querys = [q for q in querys if q not in simple_querys]

    # 2) value が非数値の場合
    simple_querys = [q for q in querys if not q.split(' ')[1].replace('.', '', 1).isdigit()]
    for query in simple_querys:
        test_name, test_value = query.split(' ')[:2]
        # 数値ではなく文字で書かれている場合
        if test_value.find("高値") != -1:
            norm_querys.append(test_name + "_high")
        # 数値ではなく文字で書かれている場合
        elif test_value.find("低値") != -1:
            norm_querys.append(test_name + "_low")
        else:
            norm_querys.append(query)
    querys = [q for q in querys if q not in simple_querys]

    # 3) 血圧の処理: 別途処理　-> 血圧query とそれ以外に分割する
    bp_querys = [q for q in querys if q.split(' ')[0] == '血圧']
    bp_cnt, bp_flags = 0, []
    for query in bp_querys:
        # 血圧の処理: 別途処理
        # 最初が収縮時血圧、最後が拡張機血圧だと信じます
        value = float(query.split(" ")[1])
        if bp_cnt == 0 and value < 120:
            bp_flags.append(0)
        elif bp_cnt == 0 and value >= 120:
            bp_flags.append(1)
        elif bp_cnt == 1 and value < 80:
            bp_flags.append(0)
        else:
            bp_flags.append(1)
        bp_cnt += 1
        if len(bp_querys) == bp_cnt:  # bp_querys の最後の処理
            # 全部チェックして異常値が無ければ normal
            if np.array(bp_flags).sum() == 0:
                norm_querys.append("血圧_normal")
            else:
                norm_querys.append("血圧_high")
                norm_querys.append("blood_pressure_abnormal")
    querys = [q for q in querys if q not in bp_querys]

    # 4) 上記以外
    for query in querys:
        # print(query)
        # 検査値かの確認
        if mojimoji.han_to_zen(query.split(" ")[0]) not in test_value_df["name"].tolist():
            norm_querys.append(query)
        else:
            try:
                test_name, test_value =\
                    query.split(" ")[0], float(query.split(" ")[1])
            # それ以外は弾く
            except:
                norm_querys.append(query)
                continue
            test_name = mojimoji.han_to_zen(test_name)
            tmp_df = test_value_df[test_value_df["name"] == test_name]
            if tmp_df.shape[0] > 0:
                test_name = mojimoji.zen_to_han(test_name)
                # 欠損の可能性がある
                try:
                    high = float(tmp_df["上限"].iloc[0])
                except:
                    high = 10000000
                try:
                    low = float(tmp_df["下限"].iloc[0])
                except:
                    low = -10000000
                if test_value > high:
                    norm_querys.append(test_name + "_high")
                elif test_value < low:
                    norm_querys.append(test_name + "_low")
                else:
                    norm_querys.append(test_name + "_normal")
            else:
                norm_querys.append(query)
    return norm_querys


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--init_db', action='store_true', help='initialize database')
#    parser.add_argument('--resource_version', type=str, default=None, help='init_dbする際のリソースバージョンを指定する')
    args = parser.parse_args()
#    if args.resource_version is not None:
#        UMLS_DB_PATH = args.resource_version
    try:
        main()
        print('end of process.')
        sys.exit(0)
    except Exception as e:
        print("Failed to create standard dictionary.", e.args, file=sys.stderr)
        sys.exit(1)

