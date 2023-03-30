"""
Configuration Params
"""
from os import environ, path
from dotenv import load_dotenv

ENV = environ.get("ENV", "stage")

basedir = path.abspath(path.dirname(__file__))

load_dotenv(path.join(basedir, "configs/" f"{ENV}.env"))
