import argparse
import os
import csv
import jaconv
import re
from umls_mapping.text2umls import is_harf
import sys

# データ行の最後に'|' が入っているので、'|'でスプリットした最後は必ず''になる。（''が一つ余計に入る)
CUI = 0
MRCONS_LANG = 1
MRCONS_STR = 14
TARGET_LANG = ['JPN', 'ENG']
TARGET_LANG_EXT = ['JPN_p', 'ENG_p', 'JPN', 'ENG']
MRDEFF_SRC = 4
MRDEFF_DEF = 5
MRSTY_STY = 3
TERM_STATUS = 2


if __name__ == '__main__':
    # 引数を処理する
    file_body = os.path.splitext(os.path.basename(__file__))[0]
    parse = argparse.ArgumentParser(description=file_body)
    parse.add_argument('--data_root', type=str, default='/mnt/e/datasets/UMLS/2022AA/META')
    parse.add_argument('--concept_source', type=str, default='MRCONSO.RRF')
    parse.add_argument('--def_source', type=str, default='MRDEF.RRF')
    parse.add_argument('--sty_source', type=str, default='MRSTY.RRF')
    parse.add_argument('--output_dir', type=str, default='resource_2022AA')
    args = parse.parse_args()

    # "CUI-表記"辞書の作成
    cui_dict = {}
    cui_dict_rep = {}
    with open(os.path.join(args.data_root, args.concept_source), mode='r', encoding='utf_8') as sf:
        reader = csv.reader(sf, delimiter='|', lineterminator='\n')
        for ws in reader:
            cui = ws[CUI]
            if cui not in cui_dict:
                # if len(cui_dict) >= 10:
                #     break
                cui_dict[cui] = {}
                cui_dict_rep[cui] = {}
                for key in TARGET_LANG_EXT:
                    cui_dict[cui][key] = []
                    cui_dict_rep[cui][key] = []
            # TARGET_LANG の文字列だけを処理する
            if ws[MRCONS_LANG] in TARGET_LANG:
                tmp = ws[MRCONS_STR]
                preferred = ws[TERM_STATUS]
                ext = ''
                if preferred == 'P' or preferred == 'p':
                    ext = '_p'
                lang = ws[MRCONS_LANG]+ext
                if ws[MRCONS_LANG] == 'JPN':
                    if is_harf(tmp):
                        # 日本語で全部半角の文字列は読み仮名。読み仮名は simstring の key から除外する
                        continue
                        # 日本語の半角文字(半角カナは全角に揃えておく)
                        # tmp = jaconv.h2z(tmp, digit=False, ascii=False)
                    # 日本語の全角英数文字は半角英数文字(lower)に揃えておく
                    tmp = jaconv.z2h(tmp, kana=False, digit=True, ascii=True)
                # 英数文字は lower に揃える。";" の有無による違いは無視する。
                tmp_rep = tmp.replace(';', ' ')
                # 連続するスペースは一つのスペースにする
                tmp_rep = re.sub(r' (2,)', ' ', tmp_rep)
                tmp = tmp_rep.lower()
                if tmp not in cui_dict[cui][lang]:
                    cui_dict[cui][lang].append(tmp)
                    cui_dict_rep[cui][lang].append(tmp_rep)

    # "CUI-DEF"辞書の作成
    def_dict = {}
    with open(os.path.join(args.data_root, args.def_source), mode='r', encoding='utf_8') as sf:
        reader = csv.reader(sf, delimiter='|', lineterminator='\n')
        for ws in reader:
            cui = ws[CUI]
            if cui not in def_dict:
                # if len(def_dict) >= 10:
                #     break
                def_dict[cui] = {}
            def_dict[cui][ws[MRDEFF_SRC]] = ws[MRDEFF_DEF]
    # 定義が空のデータが存在しないことを確認する
    assert len([k for k, v in def_dict.items() if len(v) == 0]) == 0,\
        'non defined: {}'.format([k for k, v in def_dict.items() if len(v) == 0])
    # assert len(cui_dict) == len(def_dict),\
    #     'len(cui_dict) not match len(def_dict): {}, {}'.format(len(cui_dict), len(def_dict))

    # "CUI-Semantic_Types"辞書の作成
    sty_dict = {}
    with open(os.path.join(args.data_root, args.sty_source), mode='r', encoding='utf_8') as sf:
        reader = csv.reader(sf, delimiter='|', lineterminator='\n')
        for ws in reader:
            cui = ws[CUI]
            if cui not in sty_dict:
                # if len(sty_dict) >= 10:
                #    break
                sty_dict[cui] = []
            sty_dict[cui].append(ws[MRSTY_STY])
    # Semantic typeが空のデータが存在しないことを確認する
    assert len([k for k, v in sty_dict.items() if len(v) == 0]) == 0,\
        'non defined: {}'.format([k for k, v in sty_dict.items() if len(v) == 0])
    # assert len(cui_dict) == len(sty_dict),\
    #     'len(cui_dict) not match len(sem_dict): {}, {}'.format(len(cui_dict), len(sty_dict))

    print('len(cui_dict)\t{}\tlen(def_dict)\t{}\tlen(sty_dict)\t{}'.format(len(cui_dict), len(def_dict), len(sty_dict)))
    # ファイル出力
    work_dir = os.path.dirname(os.path.dirname(os.getcwd()))
    resource_dir = os.path.join(work_dir, 'server/src/umls_mapping', args.output_dir)
    max_len = len(cui_dict)
    cnt = 0
    with open(os.path.join(resource_dir, 'UMLS_synonyms.txt'), mode='w', encoding='utf_8', newline='\n') as of:
        writer = csv.writer(of, delimiter='\t', lineterminator='\n', quoting=csv.QUOTE_ALL)
        writer.writerow(['cui', 'SemanticType', 'synonym', 'representative'])
        for k in cui_dict:
            if len(cui_dict_rep[k][TARGET_LANG_EXT[0]]) > 0:
                representative = cui_dict_rep[k][TARGET_LANG_EXT[0]][0]
            elif len(cui_dict[k][TARGET_LANG_EXT[2]]) > 0:
                representative_rep = cui_dict_rep[k][TARGET_LANG_EXT[2]][0]
            elif len(cui_dict_rep[k][TARGET_LANG_EXT[1]]) > 0:
                representative = cui_dict_rep[k][TARGET_LANG_EXT[1]][0]
            elif len(cui_dict[k][TARGET_LANG_EXT[3]]) > 0:
                representative_rep = cui_dict_rep[k][TARGET_LANG_EXT[3]][0]
            else:
                representative = ''
            for lang in TARGET_LANG_EXT:
                for s in cui_dict[k][lang]:
                    line = [k, '/'.join(sty_dict[k]), s, representative]
                    # line += [s for s in cui_dict[k][lang]]
                    writer.writerow(line)
            of.flush()
            cnt += 1
            if cnt % 10000 == 0:
                print('{}/{}'.format(cnt, max_len))

    print('end of process.')
    sys.exit(0)
