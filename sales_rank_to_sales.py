import csv
from datetime import datetime


class run_class:
    current_day = datetime.now().day
    current_month = datetime.now().month

    float_month = current_month + (current_day / 31.0)  # 1 to 13

    multipler_for_patio = 2 - (abs((float_month - 1) - 6) / 6)  # 1 is for winter, 2 is for summer


    loc_of_data = "sales_rank_info"

    catagory_to_raw_text = {}
    catagory_to_sales_info = {}

    def __init__(self):
        self.catagory_to_raw_text["video_games"] = ["Video Games"]
        self.catagory_to_raw_text["toys"] = ["Toys & Games"]
        self.catagory_to_raw_text["tools"] = ["Tools & Home Improvement"]
        self.catagory_to_raw_text["sports"] = ["Sports & Outdoors"]
        self.catagory_to_raw_text["software"] = ["Software"]
        self.catagory_to_raw_text["pet"] = ["Pet Supplies"]
        self.catagory_to_raw_text["patio"] = ["Patio, Lawn & Garden"]
        self.catagory_to_raw_text["office"] = ["Office Products"]
        self.catagory_to_raw_text["music"] = ["Musical Instruments"]
        self.catagory_to_raw_text["kitchen"] = ["Kitchen & Dining"]
        self.catagory_to_raw_text["industrial"] = ["Industrial & Scientific"]
        self.catagory_to_raw_text["home"] = ["Home & Kitchen"]
        self.catagory_to_raw_text["health"] = ["Health & Household"]
        self.catagory_to_raw_text["grocery"] = ["Grocery & Gourmet Food"]
        self.catagory_to_raw_text["electronics"] = ["Electronics"]
        self.catagory_to_raw_text["computer"] = ["Computers & Accessories"]
        self.catagory_to_raw_text["clothing"] = ["Clothing, Shoes & Jewelry"]
        self.catagory_to_raw_text["cell_phone"] = ["Cell Phones & Accessories"]
        self.catagory_to_raw_text["beauty"] = ["Beauty & Personal Care"]
        self.catagory_to_raw_text["baby"] = ["Baby"]
        self.catagory_to_raw_text["automotive"] = ["Automotive"]
        self.catagory_to_raw_text["arts"] = ["Arts, Crafts & Sewing"]
        self.catagory_to_raw_text["appliances"] = ["Appliances"]

        for cat in self.catagory_to_raw_text:
            with open(self.loc_of_data + "/" + cat + ".txt", encoding="utf8") as fd:
                rd = csv.reader(fd, delimiter="\t", quotechar='"')
                sales_rank = []
                sales = []
                for row in rd:
                    if row[0] == "" and row[1] == "":
                        t = 6
                    else:
                        sales_rank.append(float(row[0]))
                        if cat == "patio":
                            sales.append(float(row[1]) * self.multipler_for_patio)
                        else:
                            sales.append(float(row[1]))
                self.catagory_to_sales_info[cat] = {"sales_rank": sales_rank, "sales": sales}

    def check_if_cat_exits(self, catagory):
        lower_cat = catagory.lower()
        real_cat = ""
        for cat in self.catagory_to_raw_text:
            list = self.catagory_to_raw_text[cat]
            if_break = False
            for spot in list:
                if spot.lower() == lower_cat:
                    if_break = True
                    real_cat = cat
                    break
            if if_break == True:
                break

        if real_cat == "":
            return False
        else:
            return True

    def get_sales_cat(self, real_rank, catagory):

        lower_cat = catagory.lower()
        real_cat = ""
        for cat in self.catagory_to_raw_text:
            list = self.catagory_to_raw_text[cat]
            if_break = False
            for spot in list:
                if spot.lower() == lower_cat:
                    if_break = True
                    real_cat = cat
                    break
            if if_break == True:
                break

        if real_cat == "":

            dict = {"sales": 0, "cat": ""}
            return dict

        else:
            sales_ranks = self.catagory_to_sales_info[real_cat]["sales_rank"]
            sales = self.catagory_to_sales_info[real_cat]["sales"]

            real_sales = 0

            first_sales_rank = sales_ranks[0]
            spot_of_last = len(sales_ranks) - 1
            last_sales_rank = sales_ranks[spot_of_last]

            if real_rank >= first_sales_rank:
                real_sales = sales[0]
                dict = {"sales": real_sales, "cat": real_cat}
                return dict

            prev_rank = first_sales_rank
            prev_sales = sales[0]
            for i in range(1, len(sales_ranks)):
                cur_rank = sales_ranks[i]
                cur_sales = sales[i]
                if real_rank < prev_rank and real_rank >= cur_rank:
                    slope = (prev_sales - cur_sales) / (prev_rank - cur_rank)
                    real_sales = (slope * (real_rank - prev_rank)) + prev_sales
                    break
                prev_rank = cur_rank
                prev_sales = cur_sales

            dict = {"sales": real_sales, "cat": real_cat}
            return dict
