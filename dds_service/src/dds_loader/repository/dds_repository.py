from datetime import datetime

from lib.pg import PgConnect

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def restaurant_update(self,
                          restaurant_id: int,
                          name: str,
                          load_dt: datetime,
                          load_src: str
                          ):
        try:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('select 1')
        except Exception as e:
            print(e)
            conn.rollback()
            raise e
        finally:
            conn.close()

    def user_update(self,
                          user_id: int,
                          name: str,
                          load_dt: datetime,
                          load_src: str
                          ):
        try:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('select 1')
        except Exception as e:
            print(e)
            conn.rollback()
            raise e
        finally:
            conn.close()

    def user_update(self,
                          user_id: int,
                          name: str,
                          load_dt: datetime,
                          load_src: str
                          ):
        try:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('select 1')
        except Exception as e:
            print(e)
            conn.rollback()
            raise e
        finally:
            conn.close()

    def product_update(self,
                          product_id: int,
                          name: str,
                          category: str,
                          load_dt: datetime,
                          load_src: str
                          ):
        try:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('select 1')
        except Exception as e:
            print(e)
            conn.rollback()
            raise e
        finally:
            conn.close()