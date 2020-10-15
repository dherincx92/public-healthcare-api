'''
Prefect tasks to retrieve AHRQ data

author: derek663@gmail.com
last_updated: 10/05/2020
'''

import re
import requests

from bs4 import BeautifulSoup
from prefect import task, Task
from prefect.engine import signals

from config import Config as cfg
from utils import create_soup_from_response

COMPENDIUM_HREF_REGEX = re.compile(r"\/chsp\/data-resources\/(.*)[0-9]{4}\.html$")
CSV_EXTENSION_REGEX = re.compile(r"\.csv$")


@task
def create_compendium_page_soup(url=cfg.AHRQ_CHSP_URL):
  '''
  Returns a `soup` object that will be ready to parse with the bs4 module

  Parameters
  ----------
  url: str
    Compendium URL to parse

  Returns
  -------
  soup: bs4.BeautifulSoup
    A BeautifulSoup object to be parsed for information
  '''
  resp = requests.get(url)

  if resp.status_code == 200:
    create_soup_from_response(resp)
  else:
    msg = """
      The compendium page failed to be retrieved. Try loading the
      `AHRQ_COMPENDIUM_URL` in your browser. It is highly likely that the URL
      changed. If this is the case, we have to adjust our configuration
    """
    raise signals.FAIL(message=msg)

@task
def parse_soup_for_compendium_hrefs(soup):
  '''
  Returns href text to append to resources URL

  Parameters
  ----------
  soup: bs4.BeautifulSoup
    Soup object formed from the compendium page

  Returns
  -------
  hrefs: list
    Href text to append to resources URL
  '''
  tags = soup.find_all(href=re.compile(COMPENDIUM_HREF_REGEX))
  hrefs = [tag['href'] for tag in tags]
  return hrefs

@task
def create_compendium_url(url=cfg.AHRQ_RESOURCES_URL, href):
  '''
  Appends href text to the resources URL, resulting in the URL link
  for specific compendium year

  Parameters
  ----------
  url: str (default: config.AHRQ_RESOURCES_URL)
    Resources URL base string
  href: str
    Href string for each compendium year

  Returns
  -------
  compendium_url: str
    URL string for a specific compendium year
  '''
  compendium_url = f'{url}/{href}'
  return compendium_url

@task
def parse_and_find_compendium_csvs(url):
  '''
  Parses the compendium page and identifies two files - a compendium file,
  containing all system metrics (as reported by AHRQ) and a linkage file,
  linking systems by their unique identifiers.
  '''

  resp = requests.get(url)

  if resp.status_code == 200:
    soup = create_soup_from_response(resp)
  else:
    msg = 'test'














