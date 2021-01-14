from selenium import webdriver

driver = webdriver.Chrome('chromedriver')

# clear DNS cache
driver.get('chrome://net-internals/#dns')
button = driver.find_elements_by_id('dns-view-clear-cache')[0]
button.click()

## flush socket pools
driver.get('chrome://net-internals/#sockets')
button = driver.find_elements_by_id('sockets-view-flush-button')[0]
button.click()

driver.quit()