from dbcon.queries import query_store_apps_companies, query_store_apps, query_live_store_apps

from config import get_logger

logger = get_logger(__name__)

TABLES_DICT = {
    # "store_apps_companies": query_store_apps_companies, still too big
    "store_apps": query_store_apps,
    "live_store_apps": query_live_store_apps,
}

def make_tsv(file_name: str):
    logger.info(f"{file_name} start")
    df = TABLES_DICT[file_name]()
    logger.info(f"{file_name} write")
    df.to_csv('data/' + file_name + '.tsv.xz', sep="\t", index=False, compression="xz")
    logger.info(f"{file_name} done")

if __name__ == "__main__":
    logger.info("start")
    for file_name in TABLES_DICT.keys():
        make_tsv(file_name)

