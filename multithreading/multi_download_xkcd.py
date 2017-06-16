#!/usr/local/bin/python3
import os
import sys
import requests
import bs4
import threading


def download_xkcd(start, end):
    """Download comics http://xkcd.com/<start> to http://xkcd.com/<end>"""
    url = "http://xkcd.com/"
    for n in range(start, end):
            print("Downloading page {}{}...".format(url, n))
            # download page
            response = requests.get(url + str(n))
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


def main():
    """Download all XKCD comics."""
    # store comics in ./xkcd
    os.makedirs("xkcd", exist_ok=True)

    # keep track of thread objects
    threads = []
    # loop through all 1900 comics, with each thread downloading 100 comics 
    for i in range(1, 100, 100):
        download_thread = threading.Thread(target=download_xkcd, args=(i, i+99))
        threads.append(download_thread)
        download_thread.start()

    # wait for all threads to end
    for thread_obj in threads:
        # this blocks the main thread until each thread terminates
        thread_obj.join()
    # does not print until all the join calls have returned
    print("Completed")



if __name__ == "__main__":
    sys.exit(main())
