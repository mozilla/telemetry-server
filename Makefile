run: histogram_tools.py
	python server.py
 
histogram_tools.py:
	./get_histogram_tools.sh

flask:
	sudo pip install flask
