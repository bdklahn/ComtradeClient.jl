"Comtrade API host url"
const domain = "comtradeapi.un.org"

"API Version"
const v = "v1"

"API types"
@enum API bulk

"Type of trade: C for commodities and S for service"
@enum typeCode C S

"Trade frequency: A for annual and M for monthly"
@enum freqCode A M

"Trade (IMTS) classifications: HS, SITC, BEC or EBOPS."
@enum clCode HS SITC BEC EBOPS

"Columns which can be encoded with much fewer posibilities than the full alphabet of strings."
const categoricals = [:typeCode, :freqCode, :flowCode, :classificationSearchCode, :classificationCode, :cmdCode, :customsCode]

"UN Classifications on economic statistics (https://unstats.un.org/unsd/classifications/Econ) HS file base url"
const HS_json_url = "https://comtradeapi.un.org/files/v1/app/reference/"