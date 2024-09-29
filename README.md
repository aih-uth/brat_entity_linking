# brat_entity_linking: UMLS CUI Annotation Add-on

## Overview
This add-on enhances the functionality of [Brat](https://brat.nlplab.org/), a text annotation software, by allowing users (annotators) to search and assign **[UMLS (Unified Medical Language System)](https://www.nlm.nih.gov/research/umls/index.html)** code (CUI: Concept Unique Identifier) to specific text spans. The add-on is designed to streamline the process of adding standardized medical terminology to annotated text, making it easier for teams to work with clinical or biomedical data.

## Features
- **Search UMLS CUI:** Quickly search for relevant UMLS codes based on text span selections.
- **Assign UMLS CUI:** Easily link text spans in the annotation interface with the appropriate UMLS codes from search results.
- **Streamlined Workflow:** Enhances the annotation process by integrating standardized medical terminology into text annotations.

## Prerequisites
Before installing and using this add-on, ensure that you have the following:
- [Brat](https://brat.nlplab.org/)
- UMLS user acoount
- [MeCab](https://taku910.github.io/mecab/) with ipdadic-utf8
- [SimString](https://github.com/chokkan/simstring)
  - You need to install the SimString library.
  - Additionally, you must build the Python binding using SWIG, which is included in the SimString package. After building, rename the resulting `simstring.py` file to `simstring_cpp.py`.

- Python modules:
  - `simstring-fast==0.3.0`
  - `mojimoji`
  - `mecab-python3`
  - `googletrans==3.1.0a0`

## Installation

### Step 1: Download and place the Add-on files
You can download the zip file from the releases page.  
After unzip the downloaded zip file, place each files as follows:

|name|place|
|--|--|
|umls_tools/|/PATH/TO/BRAT/ext_tools|
|umls_mapping/|/PATH/TO/BRAT/server/src/|

### Step 2: Edit brat files
#### /PATH/TO/BRAT/server/src/server.py

Insert two lines to ```/PATH/TO/BRAT/server/src/server.py```
```python
from umls_mapping.word2umls import UmlsMapper
UmlsMapper()
```

#### PATH_TO_BRAT/server/src/norm.py
Line 16:
```python
# insert below
from umls_mapping.word2umls import UmlsMapper
from operator import itemgetter
```

Line 221:
```python
# before
    sorted_keys = sorted(list(datas.keys()), key=lambda a: (scores.get(a, 0), a), reverse=True)
```

```python
# after
     # sort if scores are given
     if len(scores) > 0:
         sorted_keys = sorted(list(datas.keys()), key=lambda a: (scores.get(a, 0), a), reverse=True)
     else:
         sorted_keys = list(datas.keys())
```

Line 444:
```python
# before
def norm_search(database, name, collection=None, exactmatch=False):
    try:
        return _norm_search_impl(database, name, collection, exactmatch)
    except Simstring.ssdbNotFoundError as e:
        Messager.warning(str(e))
        return {
            'database': database,
            'query': name,
            'header': [],
            'items': []
        }
```

```python
# after
def _norm_search_impl_umls(database, name, collection=None, exactmatch=False,
                           document=None, edited_span=None):
    if NORM_LOOKUP_DEBUG:
        _check_DB_version(database)
    if REPORT_LOOKUP_TIMINGS:
        lookup_start = datetime.now()

    dbpath, dbunicode = _get_db_path(database, collection)
    if dbpath is None:
        # full path not configured, fall back on name as default
        dbpath = database


    # maintain map from searched names to matching IDs and scores for
    # ranking
    matched = {}
    score_by_id = {}
    score_by_str = {}

    datas = UmlsMapper.word2umls(collection=collection, document=document,
                                 annotation_id=edited_span, query_string=name,
                                 database=database)
    header = [(label, "string") for label in ["ID", 'synonym', 'SemanticType', 'score']]
    items = [[x[0], x[1][3], x[1][2]] for x in sorted(datas, reverse=True, key=lambda x: (x[1][0], x[1][4]))]
    if REPORT_LOOKUP_TIMINGS:
        _report_timings(database, lookup_start,
                        ", retrieved " + str(len(items)) + " items")

    json_dic = {
        'database': database,
        'query': name,
        'header': header,
        'items': items,
    }
    return json_dic


def norm_search(database, name, collection=None, exactmatch=False, document=None, edited_span=None):
    try:
        if database == 'UMLS':
            return _norm_search_impl_umls(database, name, collection, exactmatch, document, edited_span)
        else:
            return _norm_search_impl(database, name, collection, exactmatch)
    except Simstring.ssdbNotFoundError as e:
        Messager.warning(str(e))
        return {
            'database': database,
            'query': name,
            'header': [],
            'items': []
        }
```


#### PATH_TO_BRAT/tools/norm_db_init.py

Line 268:
```python
# before
        try:
            connection = sqlite.connect(sqldbfn)
```

```python
# after
        try:
            connection = sqlite.connect(sqldbfn, isolation_level='EXCLUSIVE')
```

Line 370-371:
```python
# before
             if arg.verbose and (i + 1) % 10000 == 0:
                 print('.', end=' ', file=sys.stderr)
```

```python
# after
           if (i + 1) % 100000 == 0:
                connection.commit()
                if arg.verbose:
                    print('{}'.format(import_count), end=' ', file=sys.stderr)

	    connection.commit()
```

Line 407:
```python
# before
        except BaseException:
            print("Error building simstring DB", file=sys.stderr)
            raise
```

```python
# after
        except BaseException:
            connection.rollback()
            print("Error building simstring DB", file=sys.stderr)
            raise
```

Line 413:
```python
# before
        if arg.verbose:
            print("done.", file=sys.stderr)

        cursor.close()
```

```python
# after
        if arg.verbose:
            print("done.", file=sys.stderr)

        connection.commit()
        cursor.close()
```

#### PATH_TO_BRAT/static/style-ui.css

Line 142:
```css
# before
.rowselectable {
  height: 200px;
  ...
}
```

```css
# after
.rowselectable {
  height: 260px;
  ...
}
```

Line 165:
```css
# before
.rowselectable tr.selected {
  background-color: #cccccc;
}
#norm_search_container {
```

```css
# after
.rowselectable tr.selected {
  background-color: #cccccc;
}
#norm_search_result_select td {
  height: 12px;
}
#norm_search_result_select td {
  overflow-x: auto;
}
#norm_search_result_select td:nth-child(2) {
  max-width: 180px;
}
#norm_search_result_select td:nth-child(3) {
  max-width: 240px;
}
#norm_search_result_select td:nth-child(4) {
  max-width: 160px;
}
#norm_search_container {
```

### Step 3: Construct databases
Download [UMLS Metathesaurus](https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html) and obtain **MRCONSO.RRF**, **MRDEF.RRF**, and **MRSTY.RRF**.

```
cd /PATH/TO/BRAT

# build database for brat
python ext_tools/umls_tools/convert_umls2brat.py
python tools/norm_db_init.py

# build simstring index
python ext_tools/umls_tools/convert_umls2simstring.py
python server/src/umls_mapping/text2umls.py --init-db
```
### Step 4: Brat configration
Add line to tool.conf:
```
[normalization]
UMLS    DB:UMLS, <URL>:https://www.nlm.nih.gov/research/umls/index.html, <URLBASE>:http://purl.bioontology.org/ontology/MEDLINEPLUS/%s
```

Specify entity types to be annotated CUI in annotation.conf:
```
[entities]
state <NORM>:UMLS
```
