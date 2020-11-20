from pandas import read_csv, merge
import numpy


def five_percentage(df):
    rating_sum = numpy.sum(df.rating_count)
    return ((numpy.sum(df.rating_five_count) / rating_sum) * 100
            if rating_sum else 0)


def main():
    df = read_csv('data/test-task_dataset_summer_products.csv')
    groupped = df[["origin_country",
                   "rating_five_count",
                   "rating_count",
                   "price"]].groupby("origin_country")
    return merge(
        groupped['price'].mean().reset_index(name='Average price of product'),
        (groupped["rating_five_count", "rating_count"].
         apply(five_percentage).reset_index(name='Share of five-star products')),
        on='origin_country')


if __name__ == '__main__':
    print(main())
