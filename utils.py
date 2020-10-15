
from prefect.engine import signals
import requests

from bs4 import BeautifulSoup

def create_soup_from_response(response, parser='html.parser'):
  '''
  Returns a BeautifulSoup object from a response object

  Parameters
  ----------
  response: requests.models.Response
    Response object from requests

  Returns
  -------
  soup: bs4.BeautifulSoup
    Soup object to be parsed for information
  '''
  text = response.text
  soup = BeautifulSoup(text, parser)
  return soup



