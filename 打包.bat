@echo off
F:\Python27WorkSp\WebService
pyinstaller SDWebService.py

CD F:\Python27WorkSp\WebService\dist
copy /y F:\Python27WorkSp\WebService\dist\_mssql.pyd F:\Python27WorkSp\WebService\dist\SDWebService
copy /y F:\Python27WorkSp\WebService\dist\Config.ini F:\Python27WorkSp\WebService\dist\SDWebService
md F:\Python27WorkSp\WebService\dist\SDWebService\log