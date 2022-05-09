import glob
import os
_dir = os.getcwd()



def reset():
    """
    1. Resets the tracking files
    2. Deletes Existing .csv output
    """

    tracking_files = glob.glob("/home/prime/PycharmProjects/bitcoin/tracking/*")
    csv_files = glob.glob("/home/prime/PycharmProjects/bitcoin/output/*")

    print(tracking_files,'\n',
          csv_files)

    for t in tracking_files:
        with open(t, 'w') as f:
            f.write('0')

    for c in csv_files:
        if os.path.exists(c):
            os.remove(c)
        else:
            print("The file does not exist")

    print(tracking_files, '\n',
          csv_files)

reset()


#temp = ['/home/prime/PycharmProjects/bitcoin/tracking/chain_candidate.txt', '/home/prime/PycharmProjects/bitcoin/tracking/txout_detail_start_pos.txt', '/home/prime/PycharmProjects/bitcoin/tracking/txout.txt', '/home/prime/PycharmProjects/bitcoin/tracking/block_txin.txt', '/home/prime/PycharmProjects/bitcoin/tracking/multisig_pubkey.txt', '/home/prime/PycharmProjects/bitcoin/tracking/chain.txt', '/home/prime/PycharmProjects/bitcoin/tracking/pubkey_seq.txt', '/home/prime/PycharmProjects/bitcoin/tracking/pubkey.txt', '/home/prime/PycharmProjects/bitcoin/tracking/block_tx.txt', '/home/prime/PycharmProjects/bitcoin/tracking/txout_approx.txt', '/home/prime/PycharmProjects/bitcoin/tracking/orphan_block.txt', '/home/prime/PycharmProjects/bitcoin/tracking/chain_seq.txt', '/home/prime/PycharmProjects/bitcoin/tracking/block_next.txt', '/home/prime/PycharmProjects/bitcoin/tracking/block_seq.txt', '/home/prime/PycharmProjects/bitcoin/tracking/chain_summary.txt', '/home/prime/PycharmProjects/bitcoin/tracking/txin_seq.txt', '/home/prime/PycharmProjects/bitcoin/tracking/txout_seq.txt', '/home/prime/PycharmProjects/bitcoin/tracking/block.txt', '/home/prime/PycharmProjects/bitcoin/tracking/tx.txt', '/home/prime/PycharmProjects/bitcoin/tracking/txin.txt', '/home/prime/PycharmProjects/bitcoin/tracking/txout_detail.txt', '/home/prime/PycharmProjects/bitcoin/tracking/txin_detail.txt', '/home/prime/PycharmProjects/bitcoin/tracking/tx_seq.txt']


#for x in temp:
    #with open(x, 'w') as f:
        #f.write('0')
