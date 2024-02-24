import pandas as pd

def mesh_db(root):
    columns = ['pmid', 'descr', 'qualif', 'major']
    df_mesh = pd.DataFrame(columns = columns)
    count = 0
    list_of_dict = []
    for art in root.findall('PubmedArticle'):
        if count % 100 == 0:
            print(f'Processing article {count}')
        mesh = art.findall('MedlineCitation/MeshHeadingList/MeshHeading')
        for heads in mesh:
            head_dict = {}
            quals = heads.findall('QualifierName')
            q_list = []
            maj_list = []
            for qual in quals:
                q_list.append(qual.text)
                maj_list.append(qual.attrib['MajorTopicYN'])
            maj_list.append(heads.find('DescriptorName').attrib['MajorTopicYN'])
            descr = heads.find('DescriptorName').text
            head_dict['pmid'] = art.find('MedlineCitation/PMID').text
            head_dict['descr'] = descr
            head_dict['qualif'] = ','.join(q_list)
            if 'Y' in maj_list:
                head_dict['major'] = 'Y'
            else:
                head_dict['major'] = 'N'
            list_of_dict.append(head_dict)
        count+=1
    df_mesh = pd.DataFrame(list_of_dict)
    return df_mesh