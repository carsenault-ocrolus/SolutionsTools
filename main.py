from retro_export import *
from retro_flatten import *

####  GLOBALS
prj_dir = '/Users/carsenault/CustPrj/testProject/'
## MUST MANUALLY create prj_dir/outbound/OcrolusAnalytics
configure_logging(prj_dir,'logFile.txt')
num_threads = 3

def main():
    auth = get_auth('/Users/carsenault/Downloads/ocrolus_api_credentials_Arsenault Instant OAuth.json')
    book_list = get_booklist(auth,prj_dir)
    write_analytics(num_threads, book_list, auth,prj_dir)

    #### FLATTEN FILES
    flatten_analytics(prj_dir)

if __name__ == "__main__":
    main()