# собрать образ в локальный докер
docker build . -t cr.yandex/crpneac99i2mm4nqg7om/stg_service:v2025-10-14-r1
# запушить в реджистри
docker push cr.yandex/crpneac99i2mm4nqg7om/stg_service:v2025-10-14-r1
# обновить
helm upgrade --install --atomic stg-service app -n c07-timofeev-ilya

# проверить поды
kubectl get pods