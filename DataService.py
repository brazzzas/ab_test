import pandas as pd

from datetime import datetime


class DataService:

    def __init__(self, table_name_2_table):
        self.table_name_2_table = table_name_2_table

    def get_data_subset(self, table_name, begin_date, end_date, user_ids=None, columns=None):
        df = self.table_name_2_table[table_name]
        if begin_date:
            df = df[df['date'] >= begin_date]
        if end_date:
            df = df[df['date'] < end_date]
        if user_ids:
            df = df[df['user_id'].isin(user_ids)]
        if columns:
            df = df[columns]
        return df.copy()


class MetricsService:

    def __init__(self, data_service):
        """Класс для вычисления метрик.

        :param data_service (DataService): объект класса, предоставляющий доступ к данным.
        """
        self.data_service = data_service

    def _get_data_subset(self, table_name, begin_date, end_date, user_ids=None, columns=None):
        """Возвращает часть таблицы с данными."""
        return self.data_service.get_data_subset(table_name, begin_date, end_date, user_ids, columns)

    def _calculate_response_time(self, begin_date, end_date, user_ids):
        """Вычисляет значения времени обработки запроса сервером.
        
        Нужно вернуть значения user_id и load_time из таблицы 'web-logs', отфильтрованные по date и user_id.
        Считаем, что каждый запрос независим, поэтому группировать по user_id не нужно.

        :param begin_date, end_date (datetime): период времени, за который нужно считать значения.
        :param user_id (None, list[str]): id пользователей, по которым нужно отфильтровать полученные значения.
        
        :return (pd.DataFrame): датафрейм с двумя столбцами ['user_id', 'metric']
        """
        return (
            self._get_data_subset('web-logs', begin_date, end_date, user_ids, ['user_id', 'load_time'])
            .rename(columns={'load_time': 'metric'})
            [['user_id', 'metric']]
        )

    def _calculate_revenue_web(self, begin_date, end_date, user_ids):
        """Вычисляет значения выручки с пользователя за указанный период
        для заходивших на сайт в указанный период.

        Эти данные нужны для экспериментов на сайте, когда в эксперимент попадают только те, кто заходил на сайт.
        
        Нужно вернуть значения user_id и выручки (sum(price)).
        Данные о ценах в таблице 'sales'. Данные о заходивших на сайт в таблице 'web-logs'.
        Если пользователь зашёл на сайт и ничего не купил, его суммарная стоимость покупок равна нулю.
        Для каждого user_id должно быть ровно одно значение.

        :param begin_date, end_date (datetime): период времени, за который нужно считать значения.
            Также за этот период времени нужно выбирать пользователей, которые заходили на сайт.
        :param user_id (None, list[str]): id пользователей, по которым нужно отфильтровать полученные значения.
        
        :return (pd.DataFrame): датафрейм с двумя столбцами ['user_id', 'metric']
        """
        user_ids_ = (
            self._get_data_subset('web-logs', begin_date, end_date, user_ids, ['user_id'])
            ['user_id'].unique()
        )
        df = (
            self._get_data_subset('sales', begin_date, end_date, user_ids, ['user_id', 'price'])
            .groupby('user_id')[['price']].sum().reset_index() 
            .rename(columns={'price': 'metric'})
        )
        df = pd.merge(pd.DataFrame({'user_id': user_ids_}), df, on='user_id', how='left').fillna(0)
        return df[['user_id', 'metric']]

    def _calculate_revenue_all(self, begin_date, end_date, user_ids):
        """Вычисляет значения выручки с пользователя за указанный период
        для заходивших на сайт до end_date.

        Эти данные нужны, например, для экспериментов с рассылкой по email,
        когда в эксперимент попадают те, кто когда-либо оставил нам свои данные.
        
        Нужно вернуть значения user_id и выручки (sum(price)).
        Данные о ценах в таблице 'sales'. Данные о заходивших на сайт в таблице 'web-logs'.
        Если пользователь ничего не купил за указанный период, его суммарная стоимость покупок равна нулю.
        Для каждого user_id должно быть ровно одно значение.

        :param begin_date, end_date (datetime): период времени, за который нужно считать значения.
            Нужно выбирать пользователей, которые хотя бы раз заходили на сайт до end_date.
        :param user_id (None, list[str]): id пользователей, по которым нужно отфильтровать полученные значения.
        
        :return (pd.DataFrame): датафрейм с двумя столбцами ['user_id', 'metric']
        """
        user_ids_ = (
            self._get_data_subset('web-logs', None, end_date, user_ids, ['user_id'])
            ['user_id'].unique()
        )
        df = (
            self._get_data_subset('sales', begin_date, end_date, user_ids, ['user_id', 'price'])
            .groupby('user_id')[['price']].sum().reset_index() 
            .rename(columns={'price': 'metric'})
        )
        df = pd.merge(pd.DataFrame({'user_id': user_ids_}), df, on='user_id', how='left').fillna(0)
        return df[['user_id', 'metric']]

    def calculate_metric(self, metric_name, begin_date, end_date, user_ids=None):
        """Считает значения для вычисления метрик.

        :param metric_name (str): название метрики
        :param begin_date (datetime): дата начала периода (включая границу)
        :param end_date (datetime): дата окончания периода (не включая границу)
        :param user_ids (list[str], None): список пользователей.
            Если None, то вычисляет значения для всех пользователей.
        :return df: columns=['user_id', 'metric']
        """
        if metric_name == 'response time':
            return self._calculate_response_time(begin_date, end_date, user_ids)
        elif metric_name == 'revenue (web)':
            return self._calculate_revenue_web(begin_date, end_date, user_ids)
        elif metric_name == 'revenue (all)':
            return self._calculate_revenue_all(begin_date, end_date, user_ids)
        else:
            raise ValueError('Wrong metric name')
