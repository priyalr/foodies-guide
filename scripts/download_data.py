# =================================================================
# IMPORT REQUIRED LIBRARIES
# =================================================================
import config
import json
import os
import pandas as pd
import requests

# =================================================================
# READ DATA
# =================================================================

apiHeaders = {
    "user-key": config.userKey
}

cuisines_parameters = {
   "city_id": 259,
}   

search_parameters = {
   "entity_id": 259,
   "entity_type": "city",
   "sort": "rating",
   "order": "desc",
   "count": 20,
}   


raw_data_location = os.path.join(os.path.abspath(""), 'data/raw/')
cleaned_data_location = os.path.join(os.path.abspath(""), 'data/cleaned/')

# -----------------------------------------------------------------
# CUISINES
# -----------------------------------------------------------------
cuisinesResponse = requests.get("https://developers.zomato.com/api/v2.1/cuisines?", headers=apiHeaders, params=cuisines_parameters).json()

try:
    cuisinesResponse['cuisines'] # If there's an error code, it will go to except
    with open(os.path.join(raw_data_location, 'cuisines.json'), 'w') as f:
        json.dump(cuisinesResponse, f)

except:
    # If there's no data returned, don't do anything
    pass

# print(json.dumps(cuisinesResponse, sort_keys=True, indent=4))

cuisines_df = pd.json_normalize(cuisinesResponse['cuisines'])
# print(cuisines_df.head())
# print(cuisines_df.columns)
# print(cuisines_df.shape)
# print(cuisines_df)

# -----------------------------------------------------------------
# RESTAURANTS
# -----------------------------------------------------------------
num_cuisines = cuisines_df.shape[0]
# print(num_cuisines)

search_counters = [c for c in range(0, 100, 20)] # increment the start parameter by 20 each time

restaurant_list = []
restaurant_data_columns = ['restaurant.R.res_id'
                           , 'restaurant.name'
                           , 'restaurant.location.address'
                           , 'restaurant.url'
                           , 'restaurant.location.locality'
                           , 'restaurant.location.city'
                           , 'restaurant.location.latitude'
                           , 'restaurant.location.longitude'
                           , 'restaurant.location.zipcode'
                           , 'restaurant.cuisines'
                           , 'restaurant.average_cost_for_two'
                           , 'restaurant.user_rating.aggregate_rating'
                           , 'restaurant.user_rating.votes'
                           , 'restaurant.featured_image'
                           , 'restaurant.establishment'
                          ]
for i in range(num_cuisines):
# for i in range(0, 1):
    print(cuisines_df.loc[i, 'cuisine.cuisine_name'])
    curr_cuisine_id = cuisines_df.loc[i, 'cuisine.cuisine_id']
    search_parameters['cuisines'] = curr_cuisine_id
    num_cuisine_restaurants = 1
    for j in search_counters:
    # for j in [0]:
        # for restaurants that have less than 20, 40, 60, 80, 100 restaurants, don't make any extra API calls
        if j < num_cuisine_restaurants: 
            try:
                search_parameters['start'] = j
                search_response = requests.get("https://developers.zomato.com/api/v2.1/search?", headers=apiHeaders, params=search_parameters).json()
                num_cuisine_restaurants = int(search_response['results_found'])
                restaurants_df = pd.json_normalize(search_response['restaurants'])
                restaurants_df = restaurants_df[restaurant_data_columns]
                restaurants_df.insert(0, "cuisine_id", curr_cuisine_id)
                restaurant_list += restaurants_df.values.tolist()
            except Exception as e:
                print(str(e) + ". Error on cuisine" + str(i) + " search counter " + str(j))
                pass
            cuisines_df.loc[i, 'num_restaurants'] = num_cuisine_restaurants

restaurant_data_columns = ['cuisine_id'] + restaurant_data_columns
restaurants_df = pd.DataFrame(restaurant_list, columns=restaurant_data_columns)
# print(restaurants_df.head())
# print(restaurants_df.columns)
# print(restaurants_df.shape)
# print(restaurants_df)

# Convert rating to numeric so that it can be used for rannk
restaurants_df["restaurant.user_rating.aggregate_rating"] = pd.to_numeric(restaurants_df["restaurant.user_rating.aggregate_rating"])
# Rank each restaurant within a cuisine by rating
restaurants_df["rank"] = restaurants_df.groupby("cuisine_id")["restaurant.user_rating.aggregate_rating"].rank("dense", ascending=False)

# =================================================================
# OUTPUT DATA
# =================================================================
cuisines_df.to_csv(os.path.join(cleaned_data_location, 'cuisines.csv'))
restaurants_df.to_csv(os.path.join(cleaned_data_location, 'restaurants.csv'))