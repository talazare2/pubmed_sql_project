import pandas as pd

def chem_db(root):
    columns = ['pmid', 'registry_num', 'name_of_subst']
    df_chem = pd.DataFrame(columns = columns)
    count = 0
    list_dict = []
    for art in root.findall('PubmedArticle'):
        if count % 100 == 0:
            print(f'Processing article {count}')
        chem = art.findall('MedlineCitation/ChemicalList/Chemical')
        for entry in chem:
            chem_dict = {}
            chem_dict['pmid'] = art.find('MedlineCitation/PMID').text
            chem_dict['name_of_subst'] = entry.find('NameOfSubstance').text
            list_dict.append(chem_dict)
        count+=1
    df_chem = pd.DataFrame(list_dict)
    return df_chem