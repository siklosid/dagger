from flask import render_template, redirect, url_for, request, flash
from requests import exceptions as ex_requests
from app import app, es_client
from app.search_form import SearchForm



@app.route('/')
@app.route('/index')
def index():
    # readme section instead of Index
    return render_template('index.html', title='Home')


@app.route('/search', methods=['GET', 'POST'])
def search():
    form = SearchForm()
    engine_name = 'flask-app-search'
    if request.method == 'POST':
        if form.validate_on_submit():
            # Paging:
            # documents = client_app_search.search(engine_name, form.searchbox.data,
            # {"page": {"size": 25, "current": 3}})
            try:
                results = es_client.search(
                    index='index',
                    body={
                        #"query": {"match_all": {}}
                        #"query": {"match": {"_all": form.searchbox.data}}
                        "query": {
                            "query_string": {
                                "query": form.searchbox.data,
                                "fields": ["name", "description", "type"]
                            }
                        }
                    }
                )
                documents = results['hits']

            except ex_requests.ConnectionError:
                # connection error
                flash('Connection Error!! Please check connection to App-search.')
                return redirect(url_for('search'))

            # print("Method: " + request.method)
            return render_template('search.html', title='Search', form=form, search_results=documents,
                                   query=form.searchbox.data)
        else:
            return redirect(url_for('search'))
    else:
        if request.args.get('page') and request.args.get('query'):
            # print("Other: " + str(form.validate_on_submit()))
            # print("Page: " + request.args.get('page'))
            # print("Query: " + request.args.get('query'))
            # print("Method: " + request.method)
            # try:
            documents = client_app_search.search(engine_name, request.args.get('query'),
                                                     {"page": {"current": int(request.args.get('page')),
                                                               "size": app.config['POSTS_PER_PAGE']}})
            # except ex_app_search.BadRequest:
            #     # 100 pages limit or other bad requests
            #     flash('Bad request or requested more than 100 pages.')
            #     return render_template('search.html', title='Search', form=form,
            #                            query=request.args.get('query'))

            return render_template('search.html', title='Search', form=form, search_results=documents,
                                   query=request.args.get('query'))
        else:
            return render_template('search.html', title='Search', form=form)


@app.route('/graph/<string:uid>')
def graph(uid):
    return render_template(
        "graph.html",
        title="Graph",
        uid=uid,
        neo4j_host=app.config['NEO4J_HOST'],
        neo4j_port=app.config['NEO4J_PORT']
    )
