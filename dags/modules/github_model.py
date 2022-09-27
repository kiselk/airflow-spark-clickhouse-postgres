from airflow.providers.postgres.operators.postgres import PostgresOperator


class GHModel():
    model = {
        'event': {
            'type': int(),
            'actor': {'name': str()},
            'repo': {'name': str()}
        }
    }

    def __init__(self, dag, source, mode):
        self.mode = mode
        self.source = source
        self.dag = dag
        self.delete = []
        self.create = []
        self.insert = []

        self.__process_model_object(self.model)

    def __process_model_object(self, objects: dict, parent_name=None):

        for object_name, param_name in objects.items():
            if isinstance(param_name, dict):
                if parent_name:
                    if self.mode == 'create':
                        self.delete.append(self.__delete_link(
                            parent_name, object_name))
                    if self.mode == 'create':
                        self.create.append(self.__create_link(
                            parent_name, object_name))
                    if self.mode == 'insert':
                        self.insert.append(self.__insert_new_to_link(
                            parent_name, object_name))

                if self.mode == 'create':
                    self.delete.append(self.__delete_hub(object_name))
                if self.mode == 'create':
                    self.create.append(self.__create_hub(object_name))
                if self.mode == 'insert':
                    self.insert.append(self.__insert_new_to_hub(object_name))
                self.__process_model_object(param_name, object_name)
            else:
                param_name = object_name
                object_name = parent_name
                if self.mode == 'create':
                    self.delete.append(
                        self.__delete_sat(object_name, param_name))
                if self.mode == 'create':
                    self.create.append(
                        self.__create_sat(object_name, param_name))
                if self.mode == 'insert':
                    self.insert.append(
                        self.__insert_new_to_sat(object_name, param_name))

    def get_create_operators(self) -> list:
        return self.create

    def get_insert_operators(self) -> list:
        return self.insert

    def get_delete_operators(self) -> list:
        return self.delete

    def __generate_delete_hub(self, object_name: str):
        return f"""
            DROP TABLE IF EXISTS hub_{object_name} CASCADE
        """

    def __generate_delete_sat(self, object_name: str, param_name: str):
        return f"""
            DROP TABLE IF EXISTS sat_{object_name}_{param_name} CASCADE
        """

    def __generate_delete_link(self, object_name: str, param_name: str):
        return f"""
            DROP TABLE IF EXISTS link_{object_name}_{param_name} CASCADE
        """

    def __generate_create_hub(self, object_name: str):
        return f"""

            CREATE TABLE IF NOT EXISTS hub_{object_name} (
                {object_name}_id bigint PRIMARY KEY
            );

        """

    def __generate_create_sat(self, object_name: str, param_name: str):
        return f"""

            CREATE TABLE IF NOT EXISTS sat_{object_name}_{param_name} (
                {object_name}_id bigint NOT NULL,
                {object_name}_{param_name} VARCHAR NOT NULL,
                from_time timestamp
            );

        """

    def __generate_create_link(self, object_name: str, param_name: str):
        return f"""

            CREATE TABLE IF NOT EXISTS link_{object_name}_{param_name} (
                {object_name}_id bigint NOT NULL,
                {param_name}_id bigint NOT NULL
            );

        """

    def __generate_insert_hub(self, src_table_name: str, object_name: str):
        return f"""
                INSERT INTO hub_{object_name} ({object_name}_id)
                SELECT distinct a.{object_name}_id FROM {src_table_name} a
                where a.{object_name}_id not in (select distinct b.{object_name}_id from hub_{object_name} b )
        """

    def __generate_insert_sat(self, src_table_name: str, object_name: str, param_name: str):
        # return f"""
        #             INSERT INTO sat_{object_name}_{param_name} ({object_name}_id, {object_name}_{param_name}, from_time)
        #         select lt.{object_name}_id,lt.{object_name}_{param_name},lt.created_at from (
        #         select n.{object_name}_id, n.{object_name}_{param_name}, t.created_at from
        #         (  SELECT distinct {object_name}_id,{object_name}_{param_name} FROM {src_table_name}  ) n
        #         left join {src_table_name} t on t.{object_name}_{param_name} = n.{object_name}_{param_name} and t.{object_name}_id = n.{object_name}_id ) lt
        #         left join sat_{object_name}_{param_name} san on san.{object_name}_id = lt.{object_name}_id and san.{object_name}_{param_name} = lt.{object_name}_{param_name} and san.from_time < lt.created_at
        #         where san.{object_name}_id is null

        # """
        return f"""
                    INSERT INTO sat_{object_name}_{param_name} ({object_name}_id, {object_name}_{param_name}, from_time)
                    SELECT g.{object_name}_id,g.{object_name}_{param_name},latest_time from
                        (SELECT e.{object_name}_id,
                        e.{object_name}_{param_name},
                        max(created_at) as latest_time
                    FROM events e
                        group by e.{object_name}_{param_name}, e.{object_name}_id) g
                        left join sat_{object_name}_{param_name} san on san.{object_name}_id = g.{object_name}_id and san.{object_name}_{param_name} = g.{object_name}_{param_name}
                        where san.{object_name}_id is null
            """

    def __generate_insert_link(self, src_table_name: str, object_name_left: str, object_name_right: str):
        return f"""
                INSERT INTO link_{object_name_left}_{object_name_right} ({object_name_left}_id, {object_name_right}_id)
                SELECT  src.{object_name_left}_id, src.{object_name_right}_id FROM {src_table_name} src
                left join link_{object_name_left}_{object_name_right} trg on src.{object_name_left}_id = trg.{object_name_left}_id and src.{object_name_right}_id = src.{object_name_right}_id
                where trg.{object_name_left}_id is null
        """

    def __delete_hub(self, object_name: str):
        return PostgresOperator(dag=self.dag, task_id=f"delete_hub_{object_name}",
                                postgres_conn_id="postgres_default",
                                sql=self.__generate_delete_hub(object_name))

    def __delete_sat(self, object_name: str, param_name: str):
        return PostgresOperator(dag=self.dag, task_id=f"delete_sat_{object_name}_{param_name}",
                                postgres_conn_id="postgres_default",
                                sql=self.__generate_delete_sat(object_name, param_name))

    def __delete_link(self, object_name: str, param_name: str):
        return PostgresOperator(dag=self.dag, task_id=f"delete_link_{object_name}_{param_name}",
                                postgres_conn_id="postgres_default",
                                sql=self.__generate_delete_link(object_name, param_name))

    def __create_hub(self, object_name: str):
        return PostgresOperator(dag=self.dag, task_id=f"create_hub_{object_name}",
                                postgres_conn_id="postgres_default",
                                sql=self.__generate_create_hub(object_name))

    def __create_sat(self, object_name: str, param_name: str):
        return PostgresOperator(dag=self.dag, task_id=f"create_sat_{object_name}_{param_name}",
                                postgres_conn_id="postgres_default",
                                sql=self.__generate_create_sat(object_name, param_name))

    def __create_link(self, object_name: str, param_name: str):
        return PostgresOperator(dag=self.dag, task_id=f"create_link_{object_name}_{param_name}",
                                postgres_conn_id="postgres_default",
                                sql=self.__generate_create_link(object_name, param_name))

    def __insert_new_to_hub(self, object_name: str):
        return PostgresOperator(dag=self.dag, task_id=f"insert_new_to_hub_{object_name}",
                                postgres_conn_id="postgres_default",
                                sql=self.__generate_insert_hub(self.source, object_name))

    def __insert_new_to_sat(self, object_name: str, param_name: str):
        return PostgresOperator(dag=self.dag, task_id=f"insert_new_to_sat_{object_name}_{param_name}",
                                postgres_conn_id="postgres_default",
                                sql=self.__generate_insert_sat(self.source, object_name, param_name))

    def __insert_new_to_link(self, object_name: str, param_name: str):
        return PostgresOperator(dag=self.dag, task_id=f"insert_new_to_link_{object_name}_{param_name}",
                                postgres_conn_id="postgres_default",
                                sql=self.__generate_insert_link(self.source, object_name, param_name))
