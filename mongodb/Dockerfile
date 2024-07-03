FROM mongo:4.4

# Copy the initialization script into the container
COPY init_db.py /docker-entrypoint-initdb.d/

# Expose MongoDB port (27017 by default)
EXPOSE 27017

# Run the MongoDB server
CMD ["mongod"]