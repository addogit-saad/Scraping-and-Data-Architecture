from scrapper import GetData
import argparse
def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--cmd', type=str, help="""
                        default case is force and download
                        [force/download/parse.]
                            force: Force download the web data.
                            download: Download the pdf files.
                            parse: Parse the pdf files.
                        """)
    args = parser.parse_args()

    match args.cmd:
        case 'force':
            base_link = 'https://crs.agripunjab.gov.pk/reports'
            scraper = GetData(base_link, force=True)
        case 'download':
            base_link = 'https://crs.agripunjab.gov.pk/reports'
            scraper = GetData(base_link)
            scraper.download()
        case 'parse':
            raise NotImplementedError
        case _:
            base_link = 'https://crs.agripunjab.gov.pk/reports'
            scraper = GetData(base_link, force=True)
            scraper.download()

    # Parse data here

if __name__ == '__main__':
    main()