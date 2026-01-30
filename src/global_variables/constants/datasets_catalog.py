from enum import Enum

class Catalog(str,Enum):
    CLIENT = "client"
    SALES = "sales"
    #Transformation Datasets
    CLIENT_PREPARED = "client_prepared"
    SALES_PREPARED = "sales_prepared"
    SALES_ENRICHED = "sales_enriched"
    ENHANCED_CLIENT = "enhanced_client"
    UNIFIED_DATA = "unified_data"