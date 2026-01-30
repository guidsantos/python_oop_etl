from global_variables.constants.datasets_catalog import Catalog
from global_variables.constants.load_helper import FINAL_DATASET
from pipeline.load.writer import LoadSet

load_exec_contract = LoadSet(full_table_name=Catalog.UNIFIED_DATA,
                             dataset=FINAL_DATASET)