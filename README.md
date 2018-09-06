# australia-gov-au-static

Static site for www.australia.gov.au.

## Download the old site

wget in the included docker image was used to download the old site using the [download.sh](./download.sh) script.

## Modify the html

[modify.py](./modify.py) is used to tweak the downloaded files to fix up issues and convert the site to static.

## Issues in progress

### http?

Can we configure drupal to use https links rather than e.g. http://www.australia.gov.au/bowelscreening? This seems to confuse wget as it follows the 301 to:
- https://www.australia.gov.au/bowelscreening
- http://www.cancerscreening.gov.au/internet/screening/publishing.nsf/Content/bowel-campaign-home
And downloads the contents of the final site. i.e. it seems to ignore the domains
For now dealing with this with --max-redirect 0 and in modify.py

### Just remove all search bars?

- site search
- https://www.australia.gov.au/news-and-social-media/social-media/rss-feeds
- https://www.australia.gov.au/information-and-services/a-z-of-government-services

### The shortlinks seem to cause problems with get

- can we just remove them in modify.py?

### Upcoming public holiday on home page?
