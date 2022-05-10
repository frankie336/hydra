# bnk
from selenium.webdriver.common.by import By



class Locators():
    # --- Home Page Locators ---
    SEARCH_TEXTBOX=(By.ID, "twotabsearchtextbox")
    SEARCH_SUBMIT_BUTTON=(By.XPATH,"//div[contains(@class,'nav-search-submit')]/input")





    # --- Search Results Page Locators ---
    SEARCH_RESULT_LINK=(By.XPATH, "(//div[@class='sg-col-inner']//img[contains(@data-image-latency,'s-product-image')])[2]")

    # --- Product Details Page Locators ---
    ADD_TO_CART_BUTTON=(By.ID, "add-to-cart-button")

    # --- Sub Cart Page Locators ---
    SUB_CART_DIV=(By.ID,"hlb-subcart")
    PROCEED_TO_BUY_BUTTON=(By.ID,"hlb-ptc-btn-native")
    CART_COUNT=(By.ID,"nav-cart-count")
    CART_LINK=(By.ID,"nav-cart")

    # --- Cart Page Locators ---
    DELETE_ITEM_LINK=(By.XPATH,"//div[contains(@class,'a-row sc-action-links')]//span[contains(@class,'sc-action-delete')]")
    CART_COUNT=(By.ID,'nav-cart-count')
    PROCEED_TO_CHECKOUT_BUTTON=(By.NAME,"proceedToCheckout")
    # --- Signin Page Locators ---
    USER_EMAIL_OR_MOBIL_NO_TEXTBOX=(By.ID,"ap_email")





