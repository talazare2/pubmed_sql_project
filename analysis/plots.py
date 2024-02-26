import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def citations_by_year(con):
    cit_by_year = pd.read_sql_query("SELECT * FROM citations_by_year", con)
    cit_by_year=cit_by_year[cit_by_year['year']!='-']
    sns.set_theme(rc = {'figure.figsize':(20,8)})
    sns.set_theme(font_scale=1.2)
    cit_year_plot = sns.barplot(data=cit_by_year, x="year", y="cit_per_year", hue = None)
    cit_year_plot.set_xticks(cit_year_plot.get_xticks()[::10])
    cit_year_plot.set_yscale('log')
    cit_year_plot.set_xlabel("Год", fontsize=20)
    cit_year_plot.set_ylabel("Количество публикаций", fontsize=20)
    cit_year_plot.figure.savefig('/home/tiana/Desktop/mathshub_projects/sql/sql_project/analysis/results/cit_by_year.png')
    cit_year_plot.figure.clear()

def top_5_countr_over_years(con):
    top_5 = pd.read_sql_query("SELECT * FROM top_5_countries_over_years", con)
    top_5_plot = sns.lineplot(x="year", y="cit_country_year", hue="country", data=top_5)
    top_5_plot.set_yscale('log')
    top_5_plot.set_xlabel("Год")
    top_5_plot.set_ylabel("Количество публикаций")
    top_5_plot.legend()
    top_5_plot.figure.savefig('/home/tiana/Desktop/mathshub_projects/sql/sql_project/analysis/results/top_5_by_country.png')
    top_5_plot.figure.clear()

def age_cogorts(con):
    ages = pd.read_sql_query("SELECT * FROM age_cogorts", con)
    sns.set_theme(rc = {'figure.figsize':(32,10)})
    sns.set_theme(font_scale=1.6)
    age_plot = sns.violinplot(data = ages, x="descr", y="year", hue="descr")
    age_plot.set_xlabel("Возрастная когорта", fontsize = 25)
    age_plot.set_ylabel("Год", fontsize = 25)
    age_plot.figure.savefig('/home/tiana/Desktop/mathshub_projects/sql/sql_project/analysis/results/age_plot.png')
    age_plot.figure.clear()

def sex_cogorts(con):
    fm_data = pd.read_sql_query("SELECT * FROM sex_cogorts", con)
    fm_plot = sns.histplot(data=fm_data, x="year", hue="descr", 
                           multiple="stack", linewidth=.5,)
    fm_plot.set_yscale('log')
    fm_plot.set_xlabel("Год", fontsize = 25)
    fm_plot.set_ylabel("Количество публикаций", fontsize = 25)
    fm_plot.figure.savefig('/home/tiana/Desktop/mathshub_projects/sql/sql_project/analysis/results/sm_plot.png')
    fm_plot.figure.clear()

def top_3_desease(con):
    sns.set_theme(rc = {'figure.figsize':(45,10)})
    sns.set_theme(font_scale=1.3)
    top_3_data = pd.read_sql_query("SELECT * FROM top_3_desease_decade", con)
    top_3_plot = sns.catplot(data=top_3_data, kind="bar", x="year", y="citation_over_10_years", hue="descr",
                     palette="cubehelix", alpha=0.6, height=6, aspect=2, dodge=False)
    top_3_plot.set(yscale='log')
    top_3_plot.set_axis_labels("", "citations over 10 years")
    top_3_plot.legend.set_title("")
    top_3_plot.figure.savefig('/home/tiana/Desktop/mathshub_projects/sql/sql_project/analysis/results/top3.png')
    top_3_plot.figure.clear()


def do_analysis():
    con = sqlite3.connect('/home/tiana/Desktop/mathshub_projects/sql/sql_project/data/sql_db_pm', timeout=10)
    cur = con.cursor()
#    citations_by_year(con)
#    top_5_countr_over_years(con)
#    age_cogorts(con)
#    sex_cogorts(con)
    top_3_desease(con)
    con.commit()
    cur.fetchall()

do_analysis()