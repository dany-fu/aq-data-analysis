from pydent import AqSession
from tqdm import tqdm
import json
# from multiprocessing import Pool
# from multiprocessing import Process, Queue
# import time
# import concurrent.futures

prettyprint = lambda x: json.dumps(x, indent=4, sort_keys=True)


def serialize_aq_plans(aq_plan):

    # start_time = time.time()

    serialized_plan = aq_plan.dump(include={
        'operations':{
            'field_values':{
                'sample': {'opts': {'only': 'sample_type_id'}},
                'opts':{'only': 'role'}
            },
            'operation_type': {
                'opts': {'only': 'category'}
            },
            'opts': {
                'only': ('id', 'created_at', 'updated_at', 'status', 'user_id', 'operation_type_id')
            }
        },
        'wires': {
            'destination': {
                'operation': {'opts': {'only': 'id'}}
            },
            'source': {
                'operation': {'opts': {'only': 'id'}}
            },
            'opts': {'only': 'id'}
        }}, only=('id', 'created_at', 'updated_at', 'status', 'user_id'))
    # print("serialize: " + str(time.time() - start_time))
    # start_time = time.time()
    # serialized_plan = aq_plan.dump(all_relations=True)
    # print("all: " + str(time.time() - start_time))

    for op in serialized_plan['operations']:
        for fv in op['field_values']:
            if fv['sample'] is not None and fv['sample']['sample_type_id'] is not None:
                fv['sample_type'] = fv['sample']['sample_type_id']
            else:
                fv['sample_type'] = None

        if op['operation_type']['category'] is not None:
            op['category'] = op['operation_type']['category']
        else:
            op['category'] = None

    for wire in serialized_plan['wires']:
        # destination and source changes to "to" and "from" after serialization
        try:
            wire['sample'] = wire['to']['sid']
        except:
            wire['sample'] = None
        try:
            wire['to'] = wire['to']['operation']['id']
        except:
            wire['to'] = None
        try:
            wire['from'] = wire['from']['operation']['id']
        except:
            wire['from'] = None

    return serialized_plan


# def f_init(q):
#     serialize_aq_plans.q = q


if __name__ == '__main__':
    session = AqSession('danyfu', 'whiteCatsSlouchFar', 'http://0.0.0.0/')
    all_aq_plans = session.Plan.all()

    with open('all_plans_with_wires.json', 'w+') as f:
        f.write('[')
        for aq_plan in tqdm(all_aq_plans):
            f.write(prettyprint(serialize_aq_plans(aq_plan)))
            f.write(',')
        f.write(']')

    # with Pool(2) as p:
    #     json_plans = list(tqdm(p.imap(serialize_aq_plans, all_aq_plans[:50]), total=50))

    # pool = Pool()
    # pool.starmap(serialize_aq_plans, list(all_aq_plans))
    # pool.close()
    # pool.join()

    # q = Queue()
    # # p = Process(target=serialize_aq_plans, args=(q,))
    # p = Pool(None, f_init, [q])
    # p.map(serialize_aq_plans, list(all_aq_plans[:1]))
    # p.close()

    # We can use a with statement to ensure threads are cleaned up promptly
    # with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
    #     # Start the load operations and mark each future with its URL
    #     future_to_url = {executor.submit(serialize_aq_plans, aq_plan): aq_plan for aq_plan in all_aq_plans}
    #     for future in concurrent.futures.as_completed(future_to_url):
    #         url = future_to_url[future]
    #         try:
    #             data = future.result()
    #         except Exception as exc:
    #             print('%r generated an exception: %s' % (url, exc))
    #         else:
    #             json_plans.append(data)
    #             print('Appending data')
    #             # print('%r page is %d bytes' % (url, len(data)))
