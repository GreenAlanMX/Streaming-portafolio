# Dockerfile para MongoDB con script personalizado
FROM mongo:7.0

# Instalar herramientas adicionales si son necesarias
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Crear directorios para scripts
RUN mkdir -p /scripts /mongo-init

# Copiar scripts de inicialización
COPY scripts/ /scripts/
COPY mongo-init/ /docker-entrypoint-initdb.d/

# Establecer permisos de ejecución
RUN chmod +x /scripts/*.js /docker-entrypoint-initdb.d/*.sh

# Exponer puerto por defecto de MongoDB
EXPOSE 27017

# Comando por defecto (heredado de la imagen base mongo)
CMD ["mongod"]