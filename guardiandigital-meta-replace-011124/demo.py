import re
import pandas as pd
from sqlalchemy import (
    create_engine,
    update,
    MetaData,
    text,
    select,
    Table,
    Column,
    String,
    Integer,
    insert,
)
import logging
import numpy as np
import json

from bs4 import BeautifulSoup

logging.basicConfig(filename="newlog_28_march_i1.txt", level=logging.DEBUG)


import logging

formatter = logging.Formatter(
    fmt="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)


def get_logger(name, log_file, log_level, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    file = logging.FileHandler(log_file)
    file.setFormatter(formatter)
    logger.addHandler(file)
    if not log_level:
        console = logging.StreamHandler()
        # console.setFormatter(ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(console)
    return logger


import time

start_time = time.time()

# infodf = pd.read_excel('Meta_H1_Info-121123.xlsx')
infodf = pd.read_excel("new.xlsx", "New Website Plan Draft")
# mapdf = pd.read_excel('Guardian Digital - Workbook(2).xlsx', 'Meta Data Mapping')
# mapdf = pd.read_excel('GuardianDigital-Workbook-121123.xlsx', 'Meta Data Mapping')
mapdf = pd.read_excel("new.xlsx", "Meta Data Mapping")
infodf.columns = infodf.iloc[0]
infodf = infodf.iloc[1:]

infodf["map_id"] = infodf.index + 2
infodf["map_data"] = ""
infodf["meta_tag"] = False
infodf["H1_tag"] = False

# Connection parameters
# conn_params = {
#     'user': "###",
#     'password': "####",
#     'host': "####",
#     'port': 3306,
#     'database': "###"
# }

# conn_params = {
#     'user': "gdjoomla",
#     'password': "XXXXX",
#     'host': "havoc.guardiandigital.com",
#     'port': 3306,
#     'database': "gdv4webstagej4"
# }
conn_params = {
    'user': "root",
    'password': "1234",
    'host': "127.0.0.1",
    'port': 3306,
    'database': "GD"
}

# Establish a connection
# db_url = "mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s" % conn_params
# engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)

engine = create_engine(
    "mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s"
    % conn_params,
    future=True,
    pool_pre_ping=True,
    pool_recycle=3600,
)
conn = engine.connect()

meta = MetaData()
meta.reflect(bind=engine)
menu_table = pd.read_sql_query(
    text("SELECT * FROM w5zxq_menu where type <> 'url'"), conn
)
content_table = pd.read_sql_query(text("SELECT * FROM w5zxq_content"), conn)
edocman_categories_table = pd.read_sql_query(
    text("SELECT * FROM w5zxq_edocman_categories"), conn
)
categories_table = pd.read_sql_query(text("SELECT * FROM w5zxq_categories"), conn)
casestudies_table = pd.read_sql_query(text("SELECT * FROM w5zxq_casestudies_iq"), conn)
modules_table = pd.read_sql_query(
    text("SELECT * FROM w5zxq_modules where module='mod_header_iq' "), conn
)
fields_values_table = pd.read_sql_query(text("SELECT * FROM w5zxq_fields_values"), conn)


infodf["Inspiration / Current URL (if existing page)"] = infodf[
    "Inspiration / Current URL (if existing page)"
].fillna("")
infodf["Old H1"] = infodf["Old H1"].fillna("")
infodf["New Header (H1)"] = infodf["New Header (H1)"].fillna("")
infodf["Old Title"] = infodf["Old Title"].fillna("")
infodf["New Title"] = infodf["New Title"].fillna("")


infodf.replace({np.nan: ""}, inplace=True)

# Handling null
infodf["Old Metas"] = infodf["Old Metas"].fillna("")
infodf["URL"] = infodf["URL"].fillna("")

infodf["Old Metas"] = infodf["Old Metas"].apply(
    lambda x: x.replace("\\n", "\n").replace("\\r", "\r")
)

infodf["Extracted Text"] = infodf["Inspiration / Current URL (if existing page)"].apply(
    lambda url: url.split("/")[-1]
)
map_dic = {}
for i, vals in mapdf.iterrows():
    if "-" in str(vals["Rows"]):
        if vals["Rows"] == "-":
            continue
        strat = int(vals["Rows"].split("-")[0])
        end = int(vals["Rows"].split("-")[1])

        for num in range(strat, end + 1):
            map_dic[num] = [
                vals["Type"],
                vals["H1 Location (Table - Column)"],
                vals["Meta Desc Location (Table - Column)"],
            ]
    else:
        map_dic[vals["Rows"]] = [
            vals["Type"],
            vals["H1 Location (Table - Column)"],
            vals["Meta Desc Location (Table - Column)"],
        ]

# Initialize a list to store the link values
Meta_link_values = []
H1_link_values = []
Article_link_values = []
Title_link_values = []

for i, extracted_text in infodf.iterrows():
    Meta_link_value = ""
    H1_link_value = ""
    Article_link_value = ""
    Title_link_value = ""

    if extracted_text["map_id"] in map_dic:
        if extracted_text["map_id"] == 345:
            print("heeee")
        # extracted_text['map_data'] = map_dic[extracted_text['map_id']]
        infodf.loc[i, "map_data"] = str(map_dic[extracted_text["map_id"]])
        if extracted_text["URL"] == "https://guardiandigital.com/":
            infodf.loc[i, "Extracted Text"] = "home"
            extracted_text["Extracted Text"] = "home"
            extracted_text["Inspiration / Current URL (if existing page)"] = (
                "https://guardiandigital.com/home"
            )

        if ".jpg" in extracted_text["URL"]:
            match = re.search(r"/([^/]+)\?", extracted_text["URL"])
            updated_url = match.group(1)
            infodf.loc[i, "Extracted Text"] = updated_url
            extracted_text["Extracted Text"] = updated_url
            extracted_text["Inspiration / Current URL (if existing page)"] = (
                "https://guardiandigital.com/" + updated_url
            )
        if "w5zxq_menu" in map_dic[extracted_text["map_id"]][2]:
            try:
                if extracted_text["Extracted Text"] in menu_table["alias"].values:
                    # link_value = menu_table.loc[menu_table['alias'] == extracted_text['Extracted Text'], 'link'].values[0]
                    infodf.loc[i, "meta_tag"] = True
                    # Meta_link_value = map_dic[extracted_text['map_id']][2].split(' - ')[0]

                    Meta_link_h1 = menu_table.loc[
                        (menu_table["alias"] == extracted_text["Extracted Text"])
                        & (
                            menu_table["path"]
                            == extracted_text[
                                "Inspiration / Current URL (if existing page)"
                            ].split(".com/")[-1]
                        ),
                        "link",
                    ].values[0]
                    # metaMetaTable = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'params'].values[0]
                    Meta_link_id = menu_table.loc[
                        (menu_table["alias"] == extracted_text["Extracted Text"])
                        & (
                            menu_table["path"]
                            == extracted_text[
                                "Inspiration / Current URL (if existing page)"
                            ].split(".com/")[-1]
                        ),
                        "id",
                    ].values[0]
                    # print(metaMetaTable)
                    Meta_link_value = f"index.php?option=com_menu&view=metadescription&id={Meta_link_id}"
                    h1_table = (
                        map_dic[extracted_text["map_id"]][1]
                        .split("_")[1]
                        .split(" - ")[0]
                    )
                    if (
                        h1_table in Meta_link_h1
                        and h1_table == "sppagebuilder"
                        and extracted_text["Extracted Text"] != "home"
                    ):
                        infodf.loc[i, "H1_tag"] = True
                        H1_link_value = Meta_link_h1
                elif (
                    extracted_text["Extracted Text"] in content_table["alias"].values
                    and 345 <= extracted_text["map_id"] <= 366
                ):
                    infodf.loc[i, "meta_tag"] = True
                    id_content = content_table.loc[
                        content_table["alias"] == extracted_text["Extracted Text"], "id"
                    ].values[0]
                    Meta_link_value = (
                        f"index.php?option=com_content&view=article&id={id_content}"
                    )

            except Exception as e:
                Meta_link_value = ""
        if "w5zxq_menu" in map_dic[extracted_text["map_id"]][2]:
            try:
                if extracted_text["Extracted Text"] in menu_table["alias"].values:
                    # link_value = menu_table.loc[menu_table['alias'] == extracted_text['Extracted Text'], 'link'].values[0]
                    Title_link_value_id = menu_table.loc[
                        (menu_table["alias"] == extracted_text["Extracted Text"]), "id"
                    ].values[0]
                    # print(metaMetaTable)
                    Title_link_value = f"index.php?option=com_menu&view=item&client_id=0&layout=edit&id={Title_link_value_id}"

            except Exception as e:
                logging.info(f"Title extraction error:{e} ")
                Title_link_value = ""
        elif (
            "w5zxq_content" in map_dic[extracted_text["map_id"]][2]
            and extracted_text["Extracted Text"] in content_table["alias"].values
        ):
            infodf.loc[i, "meta_tag"] = True
            try:
                id_content = content_table.loc[
                    content_table["alias"] == extracted_text["Extracted Text"], "id"
                ].values[0]
                Meta_link_value = (
                    f"index.php?option=com_content&view=article&id={id_content}"
                )
                Article_link_value = (
                    f"index.php?option=com_content&view=article&id={id_content}"
                )

            except:
                Meta_link_value = ""
                Article_link_value = ""
        elif (
            "w5zxq_edocman_categories" in map_dic[extracted_text["map_id"]][2]
            and extracted_text["Extracted Text"]
            in edocman_categories_table["alias"].values
        ):
            infodf.loc[i, "meta_tag"] = True
            try:
                id_category = edocman_categories_table.loc[
                    edocman_categories_table["alias"]
                    == extracted_text["Extracted Text"],
                    "id",
                ].values[0]
                Meta_link_value = f"index.php?option=com_edocman_categories&view=categories&id={id_category}"
            except:
                Meta_link_value = ""
        elif (
            "w5zxq_casestudies_iq" in map_dic[extracted_text["map_id"]][1]
            and extracted_text["Extracted Text"] in casestudies_table["alias"].values
        ):
            infodf.loc[i, "meta_tag"] = True
            try:
                id_casestudios = casestudies_table.loc[
                    casestudies_table["alias"] == extracted_text["Extracted Text"], "id"
                ].values[0]
                Meta_link_value = f"index.php?option=com_casestudies_iq&view=casestudies&id={id_casestudios}"
            except:
                Meta_link_value = ""

        else:
            infodf.loc[i, "meta_tag"] = False
            Meta_link_value = ""

        # H1_link_value
        if (
            "w5zxq_menu" in map_dic[extracted_text["map_id"]][1]
            and extracted_text["Extracted Text"] in menu_table["alias"].values
        ):
            # link_value = menu_table.loc[menu_table['alias']   == extracted_text['Extracted Text'], 'link'].values[0]
            infodf.loc[i, "H1_tag"] = True
            try:
                # H1_link_value = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'link'].values[0]

                H1_link_value_id = menu_table.loc[
                    (menu_table["alias"] == extracted_text["Extracted Text"])
                    & (
                        menu_table["path"]
                        == extracted_text[
                            "Inspiration / Current URL (if existing page)"
                        ].split(".com/")[-1]
                    ),
                    "id",
                ].values[0]
                # print(metaMetaTable)
                H1_link_value = f"index.php?option=com_menu&view=metadescription&id={H1_link_value_id}"

            except Exception as e:

                H1_link_value = ""
                logging.error("Exception 1 occured")

        elif (
            "w5zxq_content" in map_dic[extracted_text["map_id"]][1]
            and extracted_text["Extracted Text"] in content_table["alias"].values
        ):
            infodf.loc[i, "H1_tag"] = True
            try:
                id_content = content_table.loc[
                    content_table["alias"] == extracted_text["Extracted Text"], "id"
                ].values[0]
                H1_link_value = (
                    f"index.php?option=com_content&view=article&id={id_content}"
                )

            except:
                H1_link_value = ""
                logging.error("Exception 2 occured")
        elif (
            "w5zxq_edocman_categories" in map_dic[extracted_text["map_id"]][1]
            and extracted_text["Extracted Text"]
            in edocman_categories_table["alias"].values
        ):
            infodf.loc[i, "H1_tag"] = True
            try:
                id_category = edocman_categories_table.loc[
                    edocman_categories_table["alias"]
                    == extracted_text["Extracted Text"],
                    "id",
                ].values[0]
                # H1_link_value = f'index.php?option=com_categories&view=categories&id={id_category}'
                H1_link_value = f"index.php?option=com_edocman_categories&view=categories&id={id_category}"
            except:
                H1_link_value = ""
                logging.error("Exception 3 occured")
        elif (
            "w5zxq_categories" in map_dic[extracted_text["map_id"]][1]
            and extracted_text["Extracted Text"] in categories_table["alias"].values
        ):
            infodf.loc[i, "H1_tag"] = True
            try:
                id_category = categories_table.loc[
                    categories_table["alias"] == extracted_text["Extracted Text"], "id"
                ].values[0]
                # H1_link_value = f'index.php?option=com_edocman_categories&view=categories&id={id_category}'
                H1_link_value = (
                    f"index.php?option=com_categories&view=categories&id={id_category}"
                )
            except:
                H1_link_value = ""
                logging.error("Exception 4 occured")
        elif (
            "w5zxq_casestudies_iq" in map_dic[extracted_text["map_id"]][1]
            and extracted_text["Extracted Text"] in casestudies_table["alias"].values
        ):
            infodf.loc[i, "H1_tag"] = True
            try:
                id_casestudios = casestudies_table.loc[
                    casestudies_table["alias"] == extracted_text["Extracted Text"], "id"
                ].values[0]
                H1_link_value = f"index.php?option=com_casestudies_iq&view=casestudies&id={id_casestudios}"
            except:
                H1_link_value = ""
                logging.error("Exception 5 occured")
        elif (
            "w5zxq_modules" in map_dic[extracted_text["map_id"]][1]
            and modules_table["params"]
            .str.contains(re.escape(extracted_text["Old H1"]))
            .sum()
            > 0
        ):
            infodf.loc[i, "H1_tag"] = True
            try:
                if (
                    extracted_text["Old H1"]
                    and modules_table["params"]
                    .str.contains('"' + re.escape(extracted_text["Old H1"] + '"'))
                    .sum()
                    == 1
                ):
                    id_menutable = modules_table.loc[
                        modules_table["params"].str.contains(
                            re.escape(extracted_text["Old H1"])
                        ),
                        "id",
                    ].values[0]
                    H1_link_value = (
                        f"index.php?option=com_modules&view=modules&id={id_menutable}"
                    )
            except:
                H1_link_value = ""
                logging.error("Exception 6 occured")

    Meta_link_values.append(Meta_link_value)
    H1_link_values.append(H1_link_value)
    Article_link_values.append(Article_link_value)
    Title_link_values.append(Title_link_value)


print("Meta Description is updated")


# Add the link_values list as a new column in infodf
infodf["Meta_Link"] = Meta_link_values
infodf["H1_Link"] = H1_link_values
infodf["Article_Link"] = Article_link_values
infodf["Title_Link"] = Title_link_values

# Extract values for 'id' and 'com' columns from the 'Link' column
infodf["H1_id"] = infodf["H1_Link"].apply(
    lambda link: link.split("=")[-1] if "=" in link else ""
)
infodf["H1_com"] = infodf["H1_Link"].apply(
    lambda link: (
        "w5zxq_" + link.split("com_")[1].split("&")[0] if "com_" in link else ""
    )
)
infodf["Meta_id"] = infodf["Meta_Link"].apply(
    lambda link: link.split("=")[-1] if "=" in link else ""
)
infodf["Meta_com"] = infodf["Meta_Link"].apply(
    lambda link: (
        "w5zxq_" + link.split("com_")[1].split("&")[0] if "com_" in link else ""
    )
)
infodf["Article_link"] = infodf["Article_Link"].apply(
    lambda link: (
        "w5zxq_" + link.split("com_")[1].split("&")[0]
        if "com_" and "content" in link
        else ""
    )
)
infodf["Article_id"] = infodf["Article_Link"].apply(
    lambda link: link.split("=")[-1] if "=" in link else ""
)
infodf["Title_id"] = infodf["Title_Link"].apply(
    lambda link: link.split("=")[-1] if "=" in link else ""
)
infodf["Title_com"] = infodf["Title_Link"].apply(
    lambda link: (
        "w5zxq_" + link.split("com_")[1].split("&")[0] if "com_" in link else ""
    )
)

article_ids = []
title_ids = [title_id for title_id in infodf["Title_id"]]
for article_id in infodf["Article_id"]:
    article_ids.append(article_id)


infodf["H1_Updated_DB"] = False
infodf["Meta_Updated_DB"] = False

infodf["Fields_Tables_D"] = False

infodf.to_csv("file_name55.csv")
# print(infodf)
# exit(1)
# Create a list to store the queries and results
query_results = []

updated_values = set()

w5zxq_modules = meta.tables["w5zxq_modules"]

logging.info(f"\t\t\t\t\t\tOLD\t\t\t\t\t\t\t\t\tNew")


# def update_fields_values_table()

total_records_by_type = {}
empty_records = {}
no_diff_records = {}
sppagebuilder_count = 0
for index, (
    h1_id_value,
    h1_com_value,
    meta_id_value,
    meta_com_value,
    map_id,
    map_data,
    meta_tag,
    H1_tag,
    new_H1,
    old_h1,
    new_title,
    old_title,
    old_Meta,
    New_Meta,
    title_com_value,
    title_id_value,
    record_url
) in enumerate(
    zip(
        infodf["H1_id"],
        infodf["H1_com"],
        infodf["Meta_id"],
        infodf["Meta_com"],
        infodf["map_id"],
        infodf["map_data"],
        infodf["meta_tag"],
        infodf["H1_tag"],
        infodf["New Header (H1)"],
        infodf["Old H1"],
        infodf["New Title"],
        infodf["Old Title"],
        infodf["Old Metas"],
        infodf["New Meta Description"],
        infodf["Title_com"],
        infodf["Title_id"],
        infodf["URL"]
    )
):
    logging.info(f"Row {index+1}: record_url: {record_url}, starting process")
    try:

        if h1_com_value and h1_id_value.isdigit():
            if (
                h1_com_value != "w5zxq_rsform"
                and h1_com_value != "w5zxq_blockcontent"
                and h1_com_value != "w5zxq_edocman"
            ):
                if not total_records_by_type.get(h1_com_value):
                    total_records_by_type[h1_com_value]=[f"{index +1} : {h1_id_value}"]
                else:
                    total_records_by_type[h1_com_value].append(f"{index +1} : {h1_id_value}")
                # if com_value in ('w5zxq_sppagebuilder' , 'w5zxq_content'):
                # Construct the SQL query with the table name from 'com' column
                tableComValue = meta.tables[h1_com_value]
                print(f"Processing Row {index +1}: table {tableComValue}")
                # query = select(tableComValue).where(tableComValue.c.id == h1_id_value)
                query = select(tableComValue).where(
                    tableComValue.c.id == h1_id_value
                )  # # change back to above condition

                # print(query)
                try:
                    # Execute the query to check if the table exists
                    df = pd.read_sql_query(query, conn)
                except pd.io.sql.DatabaseError as e:
                    logging.info(
                        f"Row {index + 1}: Error executing query {meta_query}: {e}"
                    )
                    # print(f"Error executing query for table '{com_value}': {e}")
                    continue

                # Check if the table exists
                if df.empty:
                    logging.info(f"Row {index + 1}: Empty Dataframe for current row")
                    # print(f"Table '{com_value}' doesn't exist. Skipping...")
                    continue

                if map_id in (401, 403, 406):
                    logging.info(f"Row {index + 1}: skipping map id")
                    print(map_id)

                for i, row in df.iterrows():
                    update_stmt = ""
                    # 1 sppage_builder --> replace of H1
                    if "sppagebuilder" in h1_com_value:

                        tablePageBuilder = meta.tables[h1_com_value]

                        column_name = re.search(r"`([^`]+)`", map_dic[map_id][1])
                        reg = '(?<=<span class=\\\\"sppb-addon-title\\\\">)[^<]+(?=<\\\\\/span>)'
                        col_text=""
                        cil_content=""
                        if column_name:
                            col_name = column_name.group(1)

                        if col_name == "text":
                            col_text = row[col_name]                            
                            search = re.search(reg, col_text)
                            if search:
                                logging.info(f"Row {index+1} h1_id: {h1_id_value}, h1_com_value : {h1_com_value}, h1 text {search} : column text found")
                            updated_string = re.sub(reg, new_H1, col_text)
                            logging.info(f"Row {index+1} h1_id: {h1_id_value}, h1_com_value : {h1_com_value} : column text updated")
                            # updated_string = text.replace(old_h1.strip(), new_H1)
                            col_content = row["content"]                            
                            search = re.search(reg, col_content)
                            if search:
                                logging.info(f"Row {index+1} h1_id: {h1_id_value}, h1_com_value : {h1_com_value}, h1 text {search} : column content found")

                            updated_content = re.sub(reg, new_H1, col_content)
                            logging.info(f"Row {index+1} h1_id: {h1_id_value}, h1_com_value : {h1_com_value} : column content updated")
                        else:
                            logging.info(f"Row {index+1} h1_id: {h1_id_value}, h1_com_value : {h1_com_value} : no column found")
                        max_spacing = 90  # Maximum spacing between columns
                        spacing = max_spacing - len(old_h1)
                        spacing1 = max_spacing - len(new_H1)
                        # Update the value in the database

                        if (updated_string != col_text or updated_content != col_content )and len(old_h1) != 0:
                            infodf.loc[
                                (infodf["map_id"] == map_id)
                                & (infodf["map_data"] == map_data),
                                "H1_Updated_DB",
                            ] = True

                            logging.info(
                                f'Row {index + 1}: Sppagebuilder Table h1: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                            )

                            # Construct the SQL update statement
                            update_stmt = (
                                update(tablePageBuilder)
                                .where(tablePageBuilder.c.id == int(h1_id_value))
                                .values(text=updated_string, content=updated_content)
                            )
                            with engine.begin() as connection:
                                connection.execute(update_stmt)
                        else:
                            updated_flag = False
                            sp_text = json.loads(text)

                            for data in sp_text:
                                # print(data)
                                for cols_data in data["columns"]:
                                    if len(cols_data["addons"]) != 0:
                                        for addons_data in cols_data["addons"]:
                                            if (
                                                "heading_selector"
                                                in addons_data["settings"]
                                            ):
                                                if (
                                                    addons_data["settings"][
                                                        "heading_selector"
                                                    ]
                                                    == "h1"
                                                ):
                                                    title = addons_data["settings"][
                                                        "title"
                                                    ]

                                                    soup = BeautifulSoup(
                                                        title, "html.parser"
                                                    )

                                                    span_tag_exists = (
                                                        soup.find("span") is not None
                                                    )
                                                    old_titile_sp = (
                                                        soup.get_text().replace(
                                                            "\n", " "
                                                        )
                                                    )
                                                    old_titile_sp = re.sub(
                                                        r"\s+", " ", old_titile_sp
                                                    ).strip()
                                                    # if span_tag_exists:
                                                    #     span_names = ['sppb-addon-title', 'fince-txt']

                                                    # text_dict = {}
                                                    # for span_name in span_names:
                                                    #     elements = soup.find_all(class_=span_name)
                                                    #     span_text = ' '.join([element.get_text() for element in elements])
                                                    #     text_dict[span_name] = span_text.replace('\n', '')
                                                    # old_titile_sp = ' '.join(text_dict.values())
                                                    # if len(old_titile_sp) == 1 or len(old_titile_sp) == 0:

                                                    # else:
                                                    #     old_titile_sp = soup.get_text().replace('\n', '')

                                                    new_title_sp = (
                                                        old_titile_sp.replace(
                                                            old_h1.strip(), new_H1
                                                        )
                                                    )
                                                    if old_titile_sp != new_title_sp:
                                                        updated_flag = True
                                                        if span_tag_exists:
                                                            addons_data["settings"][
                                                                "title"
                                                            ] = f'<span class="sppb-addon-title">{new_title_sp}</span>'
                                                        else:
                                                            addons_data["settings"][
                                                                "title"
                                                            ] = new_title_sp

                            updated_string = json.dumps(sp_text)
                            if (
                                updated_string != text
                                and updated_flag == True
                                and len(old_h1) != 0
                            ):
                                infodf.loc[
                                    (infodf["map_id"] == map_id)
                                    & (infodf["map_data"] == map_data),
                                    "H1_Updated_DB",
                                ] = True
                                logging.info(
                                    f'Row {index + 1},{h1_id_value}: Sppagebuilder Table h1: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                                )

                                # Construct the SQL update statement
                                update_stmt = (
                                    update(tablePageBuilder)
                                    .where(tablePageBuilder.c.id == int(h1_id_value))
                                    .values(text=json.dumps(sp_text))
                                )
                                with engine.begin() as connection:
                                    connection.execute(update_stmt)
                            else:
                                if not no_diff_records.get(h1_com_value):
                                    no_diff_records[h1_com_value] = [f"{index+1} : {h1_id_value}"]
                                else:
                                    no_diff_records[h1_com_value].append(f"{index+1} : {h1_id_value}")
                                logging.info(f"Row {index+1} h1_id: {h1_id_value}, h1_com_value : {h1_com_value} : old and new h1 tags are same.")
                        # logging.info(f'Sppagebuilder Table title updated: {h1_id_value}-{old_title}{" " * spacing1}{new_title}')
                        #     # Construct the SQL update statement
                        # update_stmt = update(tablePageBuilder).where(tablePageBuilder.c.id == int(h1_id_value)).values(title=new_title)
                        # with engine.begin() as connection:
                        #     connection.execute(update_stmt)
                    # 2 _content --> replace of title
                    elif "w5zxq_menu" in h1_com_value:  #
                        tablemenu = meta.tables["w5zxq_menu"]
                        params = row["params"]

                        updated_string = params.replace(old_h1.strip(), new_H1)
                        if updated_string != params:
                            infodf.loc[
                                (infodf["map_id"] == map_id)
                                & (infodf["map_data"] == map_data),
                                "H1_Updated_DB",
                            ] = True
                            logging.info(
                                f'Row {index + 1}: w5zxq_menu: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                            )
                            update_stmt = (
                                update(tablemenu)
                                .where(tablemenu.c.id == int(h1_id_value))
                                .values(params=updated_string)
                            )
                            with engine.begin() as connection:
                                connection.execute(update_stmt)
                        else:
                            if not no_diff_records.get(h1_com_value):
                                no_diff_records[h1_com_value] = [f"{index+1} : {h1_id_value}"]
                            else:
                                no_diff_records[h1_com_value].append(f"{index+1} : {h1_id_value}")
                            logging.info(f"Row {index+1} h1_id: {h1_id_value}, h1_com_value : {h1_com_value} : old and new h1 tags are same.")
                    elif "_content" in h1_com_value:  # Done for both H1 and Title
                        tableContent = meta.tables["w5zxq_content"]
                        max_spacing = 90  # Maximum spacing between columns
                        spacing = max_spacing - len(old_h1)
                        logging.info(
                            f'Row {index + 1}: Content	Table:{h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                        )
                        # Construct the SQL update statement
                        update_stmt_h1 = (
                            update(tableContent)
                            .where(tableContent.c.id == int(h1_id_value))
                            .values(title=new_H1)
                        )
                        with engine.begin() as connection:
                            connection.execute(update_stmt_h1)
                            infodf.loc[
                                (infodf["map_id"] == map_id)
                                & (infodf["map_data"] == map_data),
                                "H1_Updated_DB",
                            ] = True

                        newMeta = New_Meta
                        update_stmt_meta = (
                            update(tableContent)
                            .where(tableContent.c.id == int(h1_id_value))
                            .values(metadesc=newMeta)
                        )

                        logging.info(
                            f"Row {index + 1}: Content Meta Table:{h1_id_value} --- New Meta Description = {newMeta}"
                        )
                        # Execute the update statement
                        with engine.begin() as connection:
                            connection.execute(update_stmt_meta)
                            infodf.loc[
                                (infodf["map_id"] == map_id)
                                & (infodf["map_data"] == map_data),
                                "Meta_Updated_DB",
                            ] = True

                    # 3 _edocman_categories --> replace of title
                    elif "edocman" in h1_com_value:
                        tableCategories = meta.tables["w5zxq_edocman_categories"]
                        max_spacing = 90  # Maximum spacing between columns
                        spacing = max_spacing - len(old_h1)
                        logging.info(
                            f'Row {index + 1}: edocman_categories: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                        )
                        # Construct the SQL update statement
                        update_stmt_h1 = (
                            update(tableCategories)
                            .where(tableCategories.c.id == int(h1_id_value))
                            .values(title=new_H1)
                        )
                        with engine.begin() as connection:
                            connection.execute(update_stmt_h1)
                            infodf.loc[
                                (infodf["map_id"] == map_id)
                                & (infodf["map_data"] == map_data),
                                "H1_Updated_DB",
                            ] = True
                        # print("Value updated in the database.")

                        newMeta = New_Meta
                        update_stmt_meta = (
                            update(tableCategories)
                            .where(tableCategories.c.id == int(h1_id_value))
                            .values(metadesc=newMeta)
                        )

                        logging.info(
                            f"Row {index + 1}: edocman_categories Meta Table:{h1_id_value} --- New Meta Description = {newMeta}"
                        )
                        # Execute the update statement
                        with engine.begin() as connection:
                            connection.execute(update_stmt_meta)
                            infodf.loc[
                                (infodf["map_id"] == map_id)
                                & (infodf["map_data"] == map_data),
                                "Meta_Updated_DB",
                            ] = True

                    elif "categories" in h1_com_value:
                        max_spacing = 90
                        tableCategories = meta.tables["w5zxq_categories"]
                        description = row["description"]
                        updated_string = description.replace(old_h1.strip(), new_H1)

                        if updated_string != description:
                            infodf.loc[
                                (infodf["map_id"] == map_id)
                                & (infodf["map_data"] == map_data),
                                "H1_Updated_DB",
                            ] = True
                            logging.info(
                                f'Row {index + 1}: categories Table: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                            )

                            # Construct the SQL update statement
                            update_stmt = (
                                update(tableCategories)
                                .where(tableCategories.c.id == int(h1_id_value))
                                .values(description=updated_string)
                            )
                            with engine.begin() as connection:
                                connection.execute(update_stmt)

                        # print("Value updated in the database.")

                    # 4 __casestudies_iq --> replace of title
                    elif "casestudies_iq" in h1_com_value:
                        tableCaseStudies = meta.tables["w5zxq_casestudies_iq"]
                        max_spacing = 90  # Maximum spacing between columns
                        spacing = max_spacing - len(old_h1)
                        logging.info(
                            f'Row {index + 1}: casestudies_iq Table: {old_h1}{" " * spacing}{new_H1}'
                        )
                        # Construct the SQL update statement
                        update_stmt = (
                            update(tableCaseStudies)
                            .where(tableCaseStudies.c.id == int(h1_id_value))
                            .values(title=new_H1)
                        )
                        with engine.begin() as connection:
                            connection.execute(update_stmt)
                            infodf.loc[
                                (infodf["map_id"] == map_id)
                                & (infodf["map_data"] == map_data),
                                "H1_Updated_DB",
                            ] = True
                        # print("Value updated in the database.")

                    # 4 __casestudies_iq --> replace of title
                    elif "modules" in h1_com_value:
                        tableModules = meta.tables["w5zxq_modules"]
                        paramsJson = row["params"]
                        if old_h1 != new_H1:
                            updated_string_params = paramsJson.replace(
                                old_h1.strip(), new_H1
                            )
                            logging.info(
                                f'Row {index + 1}: Module Table: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                            )
                            # Construct the SQL update statement
                            update_stmt = (
                                update(tableModules)
                                .where(tableModules.c.id == int(h1_id_value))
                                .values(params=updated_string_params)
                            )
                            infodf.loc[
                                (infodf["map_id"] == map_id)
                                & (infodf["map_data"] == map_data),
                                "H1_Updated_DB",
                            ] = True
                            with engine.begin() as connection:
                                connection.execute(update_stmt)
                        else:
                            if not no_diff_records.get(h1_com_value):
                                no_diff_records[h1_com_value] = [f"{index+1} : {h1_id_value}"]
                            else:
                                no_diff_records[h1_com_value].append(f"{index+1} : {h1_id_value}")
                            logging.info(f"Row {index+1} h1_id: {h1_id_value}, h1_com_value : {h1_com_value} : old and new h1 tags are same.")
        if "w5zxq_menu" in title_com_value:
            tablemenu = meta.tables["w5zxq_menu"]
            row = select(tablemenu).where(tablemenu.c.id == int(title_id_value))
            df = pd.read_sql_query(row, conn)
            params = json.loads(df["params"][0])
            params["page_title"] = new_title
            updated_string = json.dumps(params)
            if updated_string != params:
                update_stmt = (
                    update(tablemenu)
                    .where(tablemenu.c.id == int(title_id_value))
                    .values(params=updated_string)
                )
                with engine.begin() as connection:
                    connection.execute(update_stmt)
                    logging.info(
                        f'Row {index + 1}: w5zxq_menu title: {title_id_value}-{old_title}{" " * 20}{new_title}'
                    )
                conn.commit()

        # Update Meta Drescription
        if meta_com_value and meta_id_value.isdigit():
            tableMetaComValue = meta.tables[meta_com_value]
            # query = select(tableComValue).where(tableComValue.c.id == h1_id_value)
            meta_query = select(tableMetaComValue).where(
                tableMetaComValue.c.id == meta_id_value
            )  # change back to above condition

            # print(query)
            try:
                # Execute the query to check if the table exists
                meta_df = pd.read_sql_query(meta_query, conn)
            except pd.io.sql.DatabaseError as e:
                logging.info(
                    f"Row {index + 1}: Error executing query {meta_query}: {e}"
                )
                # print(f"Error executing query for table '{com_value}': {e}")
                continue

            # Check if the table exists
            if meta_df.empty:
                logging.info(f"Row {index + 1}: empty dataframe")
                # print(f"Table '{com_value}' doesn't exist. Skipping...")
                continue

            for i, row in meta_df.iterrows():

                if "w5zxq_menu" in meta_com_value:
                    tablemenu = meta.tables["w5zxq_menu"]
                    params = row["params"]

                    oldMeta = json.loads(params)
                    newMeta = New_Meta
                    if newMeta[-1] == '"':
                        newMeta = newMeta[:-1]
                    oldMeta["menu-meta_description"] = newMeta

                    # updated_meta = params.replace(old_Meta, New_Meta)
                    update_stmt = (
                        update(tablemenu)
                        .where(tablemenu.c.id == int(meta_id_value))
                        .values(params=json.dumps(oldMeta))
                    )
                    infodf.loc[
                        (infodf["map_id"] == map_id) & (infodf["map_data"] == map_data),
                        "Meta_Updated_DB",
                    ] = True
                    with engine.begin() as connection:
                        connection.execute(update_stmt)

                elif (
                    "_content" in meta_com_value and 345 <= map_id <= 366
                ):  # Done for both H1 and Title
                    tableContent = meta.tables["w5zxq_content"]

                    newMeta = New_Meta
                    update_stmt_meta = (
                        update(tableContent)
                        .where(tableContent.c.id == int(meta_id_value))
                        .values(metadesc=newMeta)
                    )

                    logging.info(
                        f"Row {index + 1}: Content Meta Table:{meta_id_value} --- New Meta Description = {newMeta}"
                    )
                    # Execute the update statement
                    with engine.begin() as connection:
                        connection.execute(update_stmt_meta)
                        infodf.loc[
                            (infodf["map_id"] == map_id)
                            & (infodf["map_data"] == map_data),
                            "Meta_Updated_DB",
                        ] = True

                    # if oldMeta != params:
                    #     update_stmt = update(tablemenu).where(tablemenu.c.id == int(meta_id_value)).values(params=updated_meta )
                    #     with engine.begin() as connection:
                    #         connection.execute(update_stmt)
                    # else:
                    #     print('w5zxq_menu not updated some error')

                    # metaMetaTable = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'params'].values[0]
                    # if not pd.isna(metaMetaTable):
                    #     oldMeta = json.loads(metaMetaTable)
                    #     newMeta = extracted_text['New Meta Description']
                    #     oldMeta['menu-meta_description'] = newMeta
                    #     tableMeta = meta.tables['w5zxq_menu']

                    #     id_to_update = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'id'].values[0]
                    # update_stmt = update(tableMeta).where(tableMeta.c.id == id_to_update).values(params=json.dumps(oldMeta))

                    # logging.info(f'Menu Table: {id_to_update} \n  New Meta Description: {newMeta}')
                    # # Execute the update statement
                    # with engine.begin() as connection:
                #     connection.execute(update_stmt)

        else:
            if not empty_records.get(h1_com_value):
                empty_records[h1_com_value] = [f"{index+1} : {h1_id_value}"]
            else:
                empty_records[h1_com_value].append(f"{index+1} : {h1_id_value}")

            logging.info(f"Row {index + 1}: record_url: {record_url} neither H1 not meta description or titles updated for this record.")
        # if meta_com_value and meta_id_value.isdigit():

    except pd.io.sql.DatabaseError as e:
        print(f"Error executing query for table '{h1_com_value}': {e}")


def update_fields_values_table(article_ids):
    logger = get_logger(
        name="myfle.txt", log_file="logs/mylog.info", log_level=logging.INFO
    )

    for i in article_ids:
        if i == "":
            logger.info(f"value  of article id is  NULL ")
            continue
        else:
            new_title = infodf["New Title"][int(i)]
            infodf["field_id"] = 2
            table = meta.tables["w5zxq_fields_values"]

            insert_query = insert(table).values(field_id=2, item_id=i, value=new_title)
            # Start a transaction
            with engine.begin() as conn:
                result = conn.execute(insert_query)
                logger.info(f"field_id 2 -  item_id {i}  - value {new_title}")

            # Commit the transaction
            conn.commit()


update_fields_values_table(article_ids)
print("Text and Titles are upadted in DB")

# commit the changes into DB
connection.commit()
# Close the cursor and connection
conn.close()
end_time = time.time()

print("time: ", end_time - start_time)
logging.info(total_records_by_type)
logging.info(empty_records)
logging.info(no_diff_records)
print("total H1 records by type")
for key, value in total_records_by_type.items():
    print(f"{key}: {len(value)}")
print("\n Empty H1 records")
for key, value in empty_records.items():
    print(f"Empty : {len(value)}")
print("\n No diff H1 records")
for key, value in no_diff_records.items():
    print(f"{key}: {len(value)}")
infodf.to_csv("file_name_13_march_test6.csv")
