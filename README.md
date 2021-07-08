# Cannabis Scraper

This project was for a consulting gig I had with a company selling cannabis products accross Canada, based in Toronto.

# Description
This project solved three main problems:

1. Kept the companies CRM up to date with a source of truth of active stores based on the government listings.
2. Updated the sales team when a new store opened.
3. Leveraged outsourced data collection, to gain an idea of distribution.

The project remained live for around 18 months, until we shut it down to find a more scaleable solution.

# Store Scraper
At the time of the project, the company was selling products in 4 main provinces: British Coloumbia, Alberta, Ontario & Saskatchewan.

Each province had a unique source of store listings, in which the format varied for each data source.

This led to custom webscraping scripts for each listing.

Once the data source was scraped, the script would then cross reference it with our existing CRM and alert the sales team via Slack if a new store was listed in the data source.

This helped keep the CRM data clean while also providing actionable information to the sales team.

# Product Distribution Upload
Leveraging Upwork for outsourced labour, we had someone collect data on our products from each store from their associated websites. This was to gain an idea of who was carried our product, as the provincial governments do not provide that data. 

The end product was data containing the distribution of the product accross Canada. The sales team would then use this data to find out where to target their efforts. 

# Hosting
This repo was hosted on AWS for a period of 18 months, where the scripts would run 3x per day on a cron job. The hosting cost roughly $10.99/mo.
