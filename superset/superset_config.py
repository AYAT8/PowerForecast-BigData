import os
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
                           f"{os.environ['POSTGRES_PASSWORD']}@postgres:5432/superset")
