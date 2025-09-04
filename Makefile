submit:
	docker exec -it spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)