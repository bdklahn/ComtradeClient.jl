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