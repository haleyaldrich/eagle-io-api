import os

from bf_goodrich import itwin

os.environ["ITWIN_IOT_API_TOKEN"] = itwin.get_token()
