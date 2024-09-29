import sys
import mojimoji
import os
import re
import pandas as pd
import time
from umls_mapping import text2umls as tu


class UmlsMapper(object):
    __instance = None
    mrc_dct_jpn = None
    mrc_dct_eng = None
    m_dict = None
    rezepen_dct = None
    db_jpn = None
    test_value_df = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(UmlsMapper, cls).__new__(cls)
            cls.searcher, cls.m_dict, cls.cui2hp_dict = tu.load_dct()
            cls.test_value_df = tu.test_value_set()
        return cls.__instance

    @classmethod
    def word2umls(cls, collection, document, annotation_id, query_string='', database='UMLS'):
        # query_string は小文字に正規化しておく
        query_string = query_string.lower()

        if query_string != '':
            query_string = tu.lab_value_normalization([query_string], cls.test_value_df)[0]
            querys = [query_string]
        else:
            return []
        # querys_eng = tu.translate_Google(querys)

        scored_concept = tu.word2UMLS(querys, cls.searcher, database)
        return scored_concept


if __name__ == '__main__':
    UmlsMapper()
