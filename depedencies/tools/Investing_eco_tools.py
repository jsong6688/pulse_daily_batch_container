''' This is the main py file for the tools used in the scrapping process.
Please ensure this is imported before using the full scrapping wrapper.

'''

def get_country_mapping(path):
    ''' The purpose of this code is to get country code / mapping from
    investing.com. They have a specific country code to country map.
    '''

    content = driver.page_source
    soup = BeautifulSoup(content)

    country_ID = soup.findAll(attrs={"name" : "country[]"})

    # Need to make a mapping between country name and country code
    country_nm = []
    country_id = []
    for i in range(0,200):
        current_country = soup.findAll("label", attrs = {"for" : "country"+str(i)})
        if current_country!= []:
            country_id.append(str(i))
            country_nm.append(current_country[0].get_text())
        else:
            continue

    country_mapping = pd.DataFrame(
            {'Country_Code': country_id,
             'Country_Name': country_nm}
    )

    country_mapping.to_csv(path_or_buf=path + "\CountryMapping.csv")

def soup_get_cur(soup_result):
    ''' This is part of a small library to extract various attributes from the table
    after extracting filtered webpage content into beautiful soup format.
    This function grabs currency / country
    '''
    try:
        out = soup_result.select('span[class*="ceFlags"]')[0].get('title')
    except:
        out = []
    return out


def soup_get_importance(soup_result):
    ''' This is part of a small library to extract various attributes from the table
    after extracting filtered webpage content into beautiful soup format.
    This function grabs sentiment / importance
    '''
    out = soup_result.select('td[class*="textNum sentiment"]')[0].get_text()
    if out:
        return out
    else:
        try:
            out = soup_result.select('td[class*="textNum sentiment"]')[0].get('data-img_key') #Data image key maps to bull1/2/3
        except:
            out = []
    return out

def soup_get_event(soup_result):
    ''' This is part of a small library to extract various attributes from the table
    after extracting filtered webpage content into beautiful soup format.
    This function grabs event name
    '''
    return soup_result.select('td[class*="event"]')[0].get_text()


def soup_get_reading(soup_result):
    ''' This is part of a small library to extract various attributes from the table
    after extracting filtered webpage content into beautiful soup format.
    This function grabs reading: actual, forecast and previous
    '''
    out = []
    # Get Actual
    try:
        out.append(soup_result.select('td[id*="eventActual"]')[0].get_text())
    except:
        out.append("N/A")

    # Get Forecast
    try:
        out.append(soup_result.select('td[id*="eventForecast"]')[0].get_text())
    except:
        out.append("N/A")

    # Get Previous
    try:
        out.append(soup_result.select('td[id*="eventPrevious_"]')[0].get_text())
    except:
        out.append("N/A")
    return out

def soup_get_release_time(soup_result):
    ''' This function grabs release time for each event '''
    out = []
    # Get release time - note TR is selected from full_soup as top level so direct access is possible via key
    try:
        out.append(soup_result['data-event-datetime'])
    except:
        out.append("N/A")
    return out[0]
