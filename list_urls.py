# save a list of URIs to a file for search term

import pangaeapy.panquery as pq

search_terms = {'Sediment trap', 'Particle flux', 'Thorium'}

with open('pangae-flux-files.txt', 'w') as f:

    for s in search_terms:
        i = 0
        total = 1
        while i < total:
            q = pq.PanQuery(s, limit=100, offset=i)
            total = q.totalcount
            for r in q.result:
                print("adding DOI:", r['URI'])
                f.write(r['URI'] + '\n')
                i = i + 1

            #if i > 10:
            #    break
