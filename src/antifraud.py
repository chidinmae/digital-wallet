# -*- coding: utf-8 -*-
"""
@author: chidinma
"""

import networkx as nx
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
import os

# Create a class for payment requests
class PaymentRequest():
    def __init__(self, req):
        self.time = req[0]
        self.id1 = req[1]
        self.id2 = req[2]
        self.amount = req[3]
        self.message = req[4]
        try:
            self.time = datetime.strptime(self.time.strip(), "%Y-%m-%d %H:%M:%S")
            self.id1 = self.id1.strip()
            self.id2 = self.id2.strip()
            self.amount = Decimal(self.amount.strip())
            self.message = self.message.strip()
        except (ValueError, InvalidOperation):
            self.time = 0
            self.id1 = 0
            self.id2 = 0
            self.amount = 0
            self.message = 0     
        self.paymentrequest = (self.time, self.id1, self.id2, self.amount, self.message)

def read_batch_file(dir_path, G):
    # append input file path
    batchpaymentpath = os.path.join(dir_path, 'paymo_input', 'batch_payment.txt')
    # Read through the batch file
    with open(batchpaymentpath, 'r', encoding='utf-8') as batchreadfile:
        batchreader = csv.reader(batchreadfile, dialect=csv.unix_dialect)
        for row in batchreader:
            # account for rows with commas in the message
            if len(row)>5:
                row[4] = ",".join(row[4:len(row)])
            # drop rows that do no contain payment requests (rows created by new lines in message)
            elif len(row)<5:
                continue
            # convert row to an instance of the PaymentRequest class
            paymentreq = PaymentRequest(row[0:5])
            # skip rows with type errors
            if paymentreq.paymentrequest == (0,0,0,0,0):
                continue
            # add nodes (if not already in graph) and an edge between the points to the graph
            G.add_edge(paymentreq.id1, paymentreq.id2)
            # append or create the requests key in the edge attributes dict containing a list of requests
            if "requests" in G.edge[paymentreq.id1][paymentreq.id2]:
                G.edge[paymentreq.id1][paymentreq.id2]["requests"].append(paymentreq.paymentrequest)
            else:
                G.edge[paymentreq.id1][paymentreq.id2]["requests"] = [paymentreq.paymentrequest]
    return G
    
    
def read_stream_file(dir_path, G):
    # create file path strings
    streampaymentpath = os.path.join(dir_path, 'paymo_input', 'stream_payment.txt')
    output1path = os.path.join(dir_path, 'paymo_output', 'output1.txt')
    output2path = os.path.join(dir_path, 'paymo_output', 'output2.txt')
    output3path = os.path.join(dir_path, 'paymo_output', 'output3.txt')
    output4path = os.path.join(dir_path, 'paymo_output', 'output4.txt')
    # Process stream payment file
    with open(streampaymentpath, 'r', encoding='utf-8') as streampaymentfile:
        with open(output1path, 'w', encoding='utf-8') as output1file:
            with open(output2path, 'w', encoding='utf-8') as output2file:
                with open(output3path, 'w', encoding='utf-8') as output3file:
                    with open(output4path, 'w', encoding='utf-8') as output4file:
                        streamreader = csv.reader(streampaymentfile, dialect=csv.unix_dialect)
                        output1 = csv.writer(output1file)
                        output2 = csv.writer(output2file)
                        output3 = csv.writer(output3file)
                        output4 = csv.writer(output4file)
                        for stream in streamreader:
                            if len(stream)>5:
                                stream[4] = ",".join(stream[4:len(stream)])
                            # drop rows that do no contain payment requests
                            elif len(stream)<5:
                                continue
                            # convert stream to an instance of the PaymentRequest class
                            paymentreq = PaymentRequest(stream[0:5])
                            # skip rows with type errors
                            if paymentreq.paymentrequest == (0,0,0,0,0):
                                continue        
                            # add nodes (if not already in graph)
                            G.add_nodes_from([paymentreq.id1, paymentreq.id2])
                            # apply features
                            try:
                                shortest_path = nx.shortest_path_length(G, source=paymentreq.id1, target=paymentreq.id2)
                            except nx.NetworkXNoPath:
                                output1.writerow(["unverified"])
                                output2.writerow(["unverified"])
                                output3.writerow(["unverified"])
                            if shortest_path > 4:
                                output1.writerow(["unverified"])
                                output2.writerow(["unverified"])
                                output3.writerow(["unverified"])
                            elif shortest_path == 4 or shortest_path == 3:
                                output1.writerow(["unverified"])
                                output2.writerow(["unverified"])
                                output3.writerow(["trusted"])
                            elif shortest_path == 2:
                                output1.writerow(["unverified"])
                                output2.writerow(["trusted"])
                                output3.writerow(["trusted"])
                            elif shortest_path == 1:
                                output1.writerow(["trusted"])
                                output2.writerow(["trusted"])
                                output3.writerow(["trusted"])
                            # add edge (if not already in graph)
                            G.add_edge(paymentreq.id1,paymentreq.id2)
                            # append or create the requests key in the edge attributes dict containing a list of requests
                            if "requests" in G.edge[paymentreq.id1][paymentreq.id2]:
                                G.edge[paymentreq.id1][paymentreq.id2]["requests"].append(paymentreq.paymentrequest)
                            else:
                                G.edge[paymentreq.id1][paymentreq.id2]["requests"] = [paymentreq.paymentrequest]
                            # adding a 4th feature: check for duplicates
                            if G.edge[paymentreq.id1][paymentreq.id2]["requests"].count(paymentreq.paymentrequest) > 1:
                                output4.writerow(["unverified"])
                            else:
                                output4.writerow(["trusted"])

if __name__ == "__main__":
    # Create a graph
    G = nx.Graph()
    
    # Get directory info
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    # process batch file
    graph_with_batch_data = read_batch_file(dir_path, G)
    
    # process stream file
    read_stream_file(dir_path, graph_with_batch_data)
        