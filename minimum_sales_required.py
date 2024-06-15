def minimum_total_sales_of_search_group_for_results(results):
    points = {200: 3000, 250: 9000, 350: 14000, 400: 20000}

    order_of_keys = list(points.keys())
    order_of_keys.sort()

    if results <= order_of_keys[0]:
        return points[order_of_keys[0]]

    if results >= order_of_keys[len(order_of_keys) - 1]:
        return points[order_of_keys[len(order_of_keys) - 1]]

    for i in range(len(order_of_keys) - 1):
        x1 = order_of_keys[i]
        x2 = order_of_keys[i + 1]
        y1 = points[x1]
        y2 = points[x2]
        m = (y2 - y1) / (x2 - x1)
        if results >= x1 and results <= x2:
            return m * (results - x1) + y1


# minimum_total_sales_of_search_group_for_results(180)
