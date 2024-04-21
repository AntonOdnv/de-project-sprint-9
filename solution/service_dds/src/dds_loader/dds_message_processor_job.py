from datetime import datetime
from logging import Logger
import uuid

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        
        for _ in range(self._batch_size):
    
            msg = self._consumer.consume()
            if not msg:
                break
            
            self._logger.info(f"{datetime.utcnow()}: Message received")
            
            payload = msg['payload']

            #берем только закрытые статусы
            if payload['status'] != 'CLOSED':
                break
            self._logger.info(f"{datetime.utcnow()}: CLOSED statuses are exist")
            
            self._logger.info(f"{datetime.utcnow()}: Processing started")
            
            load_dt = datetime.now()
            load_src = self._consumer.topic

            order_id = payload['id']
            order_dt = payload['date']

            user = payload['user']
            user_id = user['id']
            username = user['name']
            userlogin = user['login']

            restaurant_id = payload['restaurant']['id']
            restaurant_name = payload['restaurant']['name']

            order_cost = payload['cost']
            order_payment = payload['payment']
            order_status = payload['status']

            products = payload['products']

            h_user_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, user_id))
            h_restaurant_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, restaurant_id))
            h_order_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(order_id)))
            hk_order_user_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_order_pk, h_user_pk])))
            hk_user_names_hashdiff = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_user_pk, username, userlogin])))
            hk_restaurant_names_hashdiff = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_restaurant_pk, restaurant_name])))
            hk_order_cost_hashdiff = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_order_pk, str(order_cost), str(order_payment)])))
            hk_order_status_hashdiff = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_order_pk, order_status])))

            self._dds_repository.h_user_loader(h_user_pk, user_id, load_dt, load_src)
            self._dds_repository.h_restaurant_loader(h_restaurant_pk, restaurant_id, load_dt, load_src)
            self._dds_repository.h_order_loader(h_order_pk, order_id, load_dt, order_dt, load_src)
            self._dds_repository.l_order_user_loader(hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
            self._dds_repository.s_user_names_loader(h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
            self._dds_repository.s_restaurant_names_loader(h_restaurant_pk, restaurant_name, load_dt, load_src, hk_restaurant_names_hashdiff)
            self._dds_repository.s_order_cost_loader(h_order_pk, order_cost, order_payment, load_dt, load_src, hk_order_cost_hashdiff)
            self._dds_repository.s_order_status_loader(h_order_pk, order_status, load_dt, load_src, hk_order_status_hashdiff)
            
            self._logger.info(f"{datetime.utcnow()}: First part (8 tables) loaded")

            h_product_pk_list = []
            h_category_pk_list = []
            product_name_list = []
            category_name_list = []
            prod_quantity_list = []

            self._logger.info(f"{datetime.utcnow()}: Start product info load")
            for product in products:
                product_id = product['id']
                category_name = product['category']
                product_name = product['name']

                h_product_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, product_id))
                h_category_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, category_name))

                hk_order_product_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_order_pk, h_product_pk])))
                hk_product_restaurant_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_product_pk, h_restaurant_pk])))
                hk_product_category_pk = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_product_pk, h_category_pk])))
                hk_product_names_hashdiff = str(uuid.uuid5(uuid.NAMESPACE_DNS, "".join([h_product_pk, product_name])))

                self._dds_repository.h_product_loader(h_product_pk, product_id, load_dt, load_src)
                self._dds_repository.h_category_loader(h_category_pk, category_name, load_dt, load_src)

                self._dds_repository.l_order_product_loader(hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                self._dds_repository.l_product_restaurant_loader(hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                self._dds_repository.l_product_category_loader(hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)

                self._dds_repository.s_product_names_loader(h_product_pk, product_name, load_dt, load_src, hk_product_names_hashdiff)

                h_product_pk_list.append(h_product_pk)
                h_category_pk_list.append(h_category_pk)
                product_name_list.append(product_name)
                category_name_list.append(category_name)
                prod_quantity_list.append(product['quantity'])

            self._logger.info(f"{datetime.utcnow()}: Second part (6 tables) loaded")
            
            self._logger.info(f"{datetime.utcnow()}: Kafka output message preparation")
            dst_msg = {
                "user_id": h_user_pk,
                "product_id": h_product_pk_list,
                "product_name": product_name_list,
                "category_id": h_category_pk_list,
                "category_name": category_name_list,
                "order_cnt": prod_quantity_list 
            }

            self._logger.info(f"{datetime.utcnow()}: Kafka output message sending")
            self._producer.produce(dst_msg)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
