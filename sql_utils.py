import sqlparse
import re


replacements = [
    {
        "pattern": r"(redshift\.)?salesforce_all.(public\.)?opportunity\d o\d? ON \w+\.loan_id = o\d?\.loan_assist_id__c( LEFT)? JOIN (redshift\.)?salesforce_all.(public\.)?opportunity\d o\d? ON \w+\.loan_id = o\d?\.loan_assist_id__c( LEFT)? JOIN (redshift\.)?salesforce_all.(public\.)?opportunity\d o\d?",
        "value": "salesforce_hevo_db.salesforce_hevo.sf_opportunity o "
    },
    {
        "pattern": r"(redshift\.)?salesforce_all.(public\.)?opportunity\d o\d? ON \w+\.loan_id = o\d?\.loan_assist_id__c( LEFT)? JOIN (redshift\.)?salesforce_all.(public\.)?opportunity\d o\d? ON \w+\.loan_id = o\d?\.loan_assist_id__c",
        "value": "salesforce_hevo_db.salesforce_hevo.sf_opportunity o "
    },
    {
        "pattern": r"(redshift\.)?salesforce_all.(public\.)?opportunity\d o\d? ON \w+\.loan_id = o\d?\.loan_assist_id__c( LEFT)? JOIN (redshift\.)?salesforce_all.(public\.)?opportunity\d o\d?",
        "value": "salesforce_hevo_db.salesforce_hevo.sf_opportunity o"
    },
    {
        "pattern": r"(redshift\.)?salesforce_all.(public\.)?opportunity\d o\d?( LEFT)? JOIN (redshift\.)?salesforce_all.(public\.)?opportunity\d o\d? ON [\w\.]+ = [\w\.]+",
        "value": "salesforce_hevo_db.salesforce_hevo.sf_opportunity o"
    },
    {
        "pattern": r"(redshift\.)?salesforce_all.(public\.)?opportunity\d o[\dp]?",
        "value": "salesforce_hevo_db.salesforce_hevo.sf_opportunity o"
    },
    {
        "pattern": r"(redshift\.)?salesforce_all.(public\.)?opportunity_field_history",
        "value": "salesforce_hevo_db.salesforce_hevo.sf_opportunityfieldhistory"
    },
    {
        "pattern": r"(\w+)\.loan_id = o[\dp]?\.loan_assist_id__c",
        "value": r"\1.loan_id::varchar = o.loan_assist_id__c"
    },
    {
        "pattern": r"(\w+)\.loan_id = o[\dp]?\.originating_loan_id__c",
        "value": r"\1.loan_id::varchar = o.originating_loan_id__c"
    },
    {
        "pattern": r"lf\.loan_id = sf\.sf_loan_id",
        "value": "lf.loan_id::varchar = sf.sf_loan_id"
    },
    {
        "pattern": r"(?<!AS \")o[1-3]",
        "value": "o"
    },
    {
        "pattern": r" op\.",
        "value": " o."
    },
]


def replace_values(sql: str) -> str:
    for replacement in replacements:
        pattern = replacement["pattern"]
        value = replacement["value"]
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            sql = re.sub(pattern, value, sql, flags=re.IGNORECASE)
    return sql


def convert_to_single_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line != '']
    return " ".join(lines)


def format_sql(sql: str) -> str:
    return sqlparse.format(sql,
                           keyword_case='upper',
                           identifier_case='lower',
                           reindent=True,
                           ident_width=2,
                           wrap_after=80,
                           use_space_around_operators=True,
                           strip_comments=True,
                           strip_whitespace=True)


def update_sql(sql: str) -> str:
    sql = format_sql(sql)
    sql = convert_to_single_line(sql)
    sql = replace_values(sql)
    sql = format_sql(sql)
    return sql
