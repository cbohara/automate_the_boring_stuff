#!/usr/local/bin/python3
import os
import sys
import requests
import bs4


def main():
    """Download all XKCD comics."""
    url = "http://xkcd.com"
    # store comics in ./xkcd
    os.makedirs("xkcd", exist_ok=True)

    while not url.endswith("#"):
        print("Downloading page {}...".format(url))
        # download page
        response = requests.get(url)
        # raise an exception if there was an error downloading the file
        response.raise_for_status()

        # create beautiful soup object in order to parse html 
        soup = bs4.BeautifulSoup(response.text)
        # find the url of the comic image
        comic_img = soup.select("#comic img")
        # if the comic_img list is empty 
        if comic_img == []:
            print("Could not find comic image.")
        else:
            comic_title = comic_img[0].get("alt")
            # get img url
            comic_url = "http:" + comic_img[0].get("src")
            # download image
            print("Downloading image {}...".format(comic_title))
            response = requests.get(comic_url)
            response.raise_for_status()

            # save the image to binary file in xkcd folder
            with open(os.path.join("xkcd", os.path.basename(comic_url)), "wb") as f:
                # grab chunks of data from response variable and write to image file
                for chunk in response.iter_content(10000):
                    f.write(chunk)


        # get the Prev button url
        prev_link = soup.select("a[rel='prev']")[0]
        url = 'http://xkcd.com' + prev_link.get('href')

    print("Done")


if __name__ == "__main__":
    sys.exit(main())
