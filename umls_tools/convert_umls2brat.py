import argparse
import os
import csv
import jaconv

# データ行の最後に'|' が入っているので、'|'でスプリットした最後は必ず''になる。（''が一つ余計に入る)
CUI = 0
MRCONS_LANG = 1
MRCONS_STR = 14
TARGET_LANG = ['JPN', 'ENG']
MRDEFF_SRC = 4
MRDEFF_DEF = 5
MRSTY_STY = 3


if __name__ == '__main__':
    # 引数を処理する
    file_body = os.path.splitext(os.path.basename(__file__))[0]
    parse = argparse.ArgumentParser(description=file_body)
    parse.add_argument('--data_root', type=str, help='path to directory which contains MRCONSO.RRF, MRDEF.RRF, and MRSTY.RRF (for example, "./UMLS/2019AB/META")')
    parse.add_argument('--concept_source', type=str, default='MRCONSO.RRF')
    parse.add_argument('--def_source', type=str, default='MRDEF.RRF')
    parse.add_argument('--sty_source', type=str, default='MRSTY.RRF')
    args = parse.parse_args()

    # "CUI-表記"辞書の作成
    cui_dict = {}
    with open(os.path.join(args.data_root, args.concept_source), mode='r', encoding='utf_8') as sf:
        reader = csv.reader(sf, delimiter='|', lineterminator='\n')
        for ws in reader:
            cui = ws[CUI]
            if cui not in cui_dict:
                # if len(cui_dict) >= 10:
                #     break
                cui_dict[cui] = {'JPN': [], 'ENG': []}
            if ws[MRCONS_LANG] in TARGET_LANG:
                tmp = ws[MRCONS_STR]
                if ws[MRCONS_LANG] == 'JPN':
                    # 日本語の半角文字(半角カナは全角に揃えておく)
                    tmp = jaconv.h2z(tmp, digit=False, ascii=False)
                    # 日本語の全角英数文字は半角英数文字に揃えておく
                    tmp = jaconv.z2h(tmp, kana=False, digit=True, ascii=True)
                if tmp not in cui_dict[cui][ws[MRCONS_LANG]]:
                    cui_dict[cui][ws[MRCONS_LANG]].append(tmp)

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
    work_dir = os.path.join(work_dir, 'work')
    max_len = len(cui_dict)
    cnt = 0
    with open(os.path.join(work_dir, 'umls_source', 'UMLS.txt'), mode='w', encoding='utf_8', newline='\n') as of:
        for k, v in cui_dict.items():
            line = [k]
            for lang in TARGET_LANG:
                line += ['name:Synonym:' + s for s in cui_dict[k][lang]]
            if k in sty_dict:
                line += ['attr:SemanticType:' + s for s in sty_dict[k]]
            if k in def_dict:
                # TODO: 仮実装、とりあえず最初に見つかった定義を採用している
                # definition に \t が入っていることがある。\t -> ' ' に変換しておく
                line.append('info:Definition:' + list(def_dict[k].values())[0].replace('\t', ' '))
            of.write('\t'.join(line) + '\n')
            of.flush()
            cnt += 1
            if cnt % 10000 == 0:
                print('{}/{}'.format(cnt, max_len))

    print('end of process.')
