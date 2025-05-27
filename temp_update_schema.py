from sqlalchemy import create_engine, text, inspect
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.info("DATABASE_URL not set, using default for this script run.")
    DATABASE_URL = "postgresql://swordfinder:swordfinder123@localhost:5432/swordfinder_db"

engine = create_engine(DATABASE_URL)

def column_exists(engine, table_name, column_name):
    """Checks if a column exists in a table."""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    for column in columns:
        if column['name'] == column_name:
            return True
    return False

if __name__ == "__main__":
    table_name = 'sword_swings'
    column_name_to_add = 'raw_sword_metric'
    column_type = 'FLOAT'

    logger.info("First, ensuring all tables are created based on models...")
    try:
        # Call create_tables from models_complete to ensure all tables exist
        # This uses Base.metadata.create_all(engine)
        from models_complete import create_tables as mc_create_tables # Alias to avoid conflict if any
        mc_create_tables()
        logger.info("create_tables() called. Tables should now exist if they didn't.")
    except Exception as e:
        logger.error(f"Error during initial create_tables() call: {e}")
        logger.error("Cannot proceed with column check if table creation fails.")
        exit()

    logger.info(f"Checking if column '{column_name_to_add}' exists in table '{table_name}'...")
    
    try:
        if not column_exists(engine, table_name, column_name_to_add):
            logger.info(f"Column '{column_name_to_add}' does not exist in '{table_name}'. Attempting to add it...")
            with engine.connect() as connection:
                try:
                    connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name_to_add} {column_type};"))
                    connection.commit() 
                    logger.info(f"Column '{column_name_to_add}' added successfully to '{table_name}'.")
                except Exception as alter_err:
                    logger.error(f"Error executing ALTER TABLE for {table_name}: {alter_err}")
                    connection.rollback() 
        else:
            logger.info(f"Column '{column_name_to_add}' already exists in table '{table_name}'. No action taken.")
    except Exception as e:
        # This might catch errors if the table itself doesn't exist after create_tables()
        logger.error(f"An error occurred during schema check/update for column '{column_name_to_add}' in table '{table_name}': {e}")
        logger.error("This might indicate the table itself ('sword_swings') was not created by create_tables().")
