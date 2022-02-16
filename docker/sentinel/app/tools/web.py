
import re


def get_filename_from_content_disposition(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0].removeprefix('"').removesuffix('"')
