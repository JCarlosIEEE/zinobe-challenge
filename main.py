import zinobe_challenge as zinobe

if __name__ == '__main__':
    url = 'https://restcountries.com/v3.1/all'
    response = zinobe.get_data(url)
    dataframe = zinobe.make_dataframe(response)
    print(dataframe.head())
    zinobe.print_statistics(dataframe)
    zinobe.create_db(dataframe)
    zinobe.create_json()
