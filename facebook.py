import requests

def getFollowersCountFB():
    followers_count = requests.get('https://graph.facebook.com/v9.0/17841400011449626/?fields=followers_count&access_token=EAAXUQ5082fwBAIsLwNPBr1xBvDVgZBvwHkcTZBBZBuSsow2lGu6keVP7PZA0kSPiR8oX7XjoWnDTlW8TCdR9auxJccKNVfhpSDheOATEdCZANRQyRGD6Xz8Gkl276cmKXuMPDgqH2TVSR5Uyo8W2YGqaZCIm3GCi8avlClzpv3JYEbn4qylsxg5aseWISUNJKIcODr4G2dfQZDZD')
    return followers_count.json()

def getInsightsMetricFB():
    insights_metric = requests.get('https://graph.facebook.com/v9.0/17841400011449626/insights?metric=impressions,reach,follower_count,email_contacts,phone_call_clicks,text_message_clicks,get_directions_clicks,website_clicks,profile_views&access_token=EAAXUQ5082fwBAIsLwNPBr1xBvDVgZBvwHkcTZBBZBuSsow2lGu6keVP7PZA0kSPiR8oX7XjoWnDTlW8TCdR9auxJccKNVfhpSDheOATEdCZANRQyRGD6Xz8Gkl276cmKXuMPDgqH2TVSR5Uyo8W2YGqaZCIm3GCi8avlClzpv3JYEbn4qylsxg5aseWISUNJKIcODr4G2dfQZDZD&date_preset=last_year&period=day')
    return insights_metric.json()

def main():
    followersCountFB = getFollowersCountFB()
    insightsMetricFB = getInsightsMetricFB()
    print(followersCountFB)
    print(insightsMetricFB)

if __name__ == '__main__':
    main()