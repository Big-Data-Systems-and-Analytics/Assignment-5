import pandas as pd


from nearbyloc import locfinder


def locator (begin_location):
    cache_df = pd.read_csv("Streamlit_Frontend/Cacheloc_Details.csv")
    if cache_df.loc[cache_df['location']==begin_location].shape[0] >= 1:
        return begin_location
    else:
        loc = locfinder(begin_location)
        if loc != "NoLoc":
            if cache_df.loc[cache_df['location']==loc].shape[0] >= 1:
                return loc
            else:
                return "hitapi"
        else:
            return "hitapi"







        