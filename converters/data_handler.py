import pandas as pd
from data.date_lex import LEXICON_DATE

def get_pmid(article):
    return article.find('MedlineCitation/PMID').text

def get_article_title(article):
    try:
        return article.find('MedlineCitation/Article/ArticleTitle').text
    except:
        return '-'
   
def get_journal(article):
    journal = {}
    journal['issn'] = article.find('MedlineCitation/Article/Journal/ISSN')
    journal['title'] = article.find('MedlineCitation/Article/Journal/Title')
    journal['volume'] = article.find('MedlineCitation/Article/Journal/JournalIssue/Volume')
    journal['issue'] = article.find('MedlineCitation/Article/Journal/JournalIssue/Issue')
    journal['year'] = article.find('MedlineCitation/Article/Journal/JournalIssue/PubDate/Year')
    journal['month'] = article.find('MedlineCitation/Article/Journal/JournalIssue/PubDate/Month')
    journal['day'] = article.find('MedlineCitation/Article/Journal/JournalIssue/PubDate/Day')
    for key, value in journal.items():
        try:
            journal[key] = value.text
        except:
            journal[key] = '-'
    return journal

def get_country(article):
    try:
        return article.find('MedlineCitation/MedlineJournalInfo/Country').text
    except:
        '-'

def db_general(root):
    columns = ['pmid', 'title', 'journ_issn', 'journ_title', 'journ_volume', 'journ_issue', 'year', 'month', 'day', 'country']
    df_gen = pd.DataFrame(columns = columns)
    count = 0
    list_dict = []
    for art in root.findall('PubmedArticle'):
        if count % 100 == 0:
            print(f'Processing article {count}')
        gen_dict = {}
        gen_dict['pmid'] = get_pmid(art)
        gen_dict['title'] = get_article_title(art)
        journal = get_journal(art)
        gen_dict['journ_issn'] = journal['issn']
        gen_dict['journ_title'] = journal['title']
        gen_dict['journ_volume'] = journal['volume']
        gen_dict['journ_issue'] = journal['issue']
        gen_dict['year'] = journal['year']
        gen_dict['month'] = journal['month']
        gen_dict['day'] = journal['day']
        gen_dict['country'] = get_country(art)
        list_dict.append(gen_dict)
        count+=1
    df_gen = pd.DataFrame(list_dict)
    for key, value in LEXICON_DATE.items():
        mask_1 = (df_gen['month'] == key)
        mask_2 = (df_gen['day'] == key)
        df_gen.loc[mask_1, ['month']] = value
        df_gen.loc[mask_2, ['day']] = value
    return df_gen

    

