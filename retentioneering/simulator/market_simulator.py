# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import pandas as pd
import numpy as np
from tqdm import trange
from collections import Counter
from matplotlib import pyplot as plt


class Agent(object):
    def __init__(self, state, adj, states_list, finance_data, atrans, aparams, plist,
                 iters=1, atype=0, pad_len=-1, negative_event='lost', positive_event='checkout'):
        self.iters = iters
        self.adj = adj.copy()
        self.finance_data = finance_data.copy()
        self.states_list = states_list
        self.track = [state]
        self.money = [0] * (pad_len + 2)
        self.atrans, self.aparams = atrans.copy(), aparams.copy()
        self._init_adaptive_(plist)
        self.atype = atype
        self.positive_event = positive_event
        self.negative_event = negative_event

    def _init_adaptive_(self, plist):
        for key, val in plist.items():
            setattr(self, key, val)
        for key, val in self.atrans.items():
            for par, func in val:
                p = [getattr(self, i) for i in par]
                self.adj.loc[key[0], key[1]] = func(*p)

    @staticmethod
    def normalize(v):
        return v / np.sum(v)

    def _update_params_(self, fr, to):
        fd = self.aparams.get((fr, to))
        if fd is None:
            return
        for j in range(len(fd)):
            p = [getattr(self, i) for i in fd[j][0]]
            for key, val in zip(fd[j][0], fd[j][1](*p)):
                setattr(self, key, val)
        self._update_adj_()

    def _update_adj_(self):
        for (fr, to) in self.atrans:
            self.__update_adj__(fr, to)

    def __update_adj__(self, fr, to):
        fd = self.atrans.get((fr, to), [])
        for j in range(len(fd)):
            p = [getattr(self, i) for i in fd[j][0]]
            self.adj.loc[fr, to] = fd[j][1](*p)

    def next_step(self, state):
        return np.random.choice(self.states_list, p=self.normalize(self.adj.loc[state]))

    def step(self):
        st = self.track[-1]
        if st == self.negative_event:
            self.money.append(0)
            self.track.append(self.negative_event)
            return self.negative_event
        elif st == self.positive_event:
            self.money.append(0)
            self.track.append(self.positive_event)
            return self.positive_event
        self.track.append(self.next_step(st))
        self.money.append(self.finance_data.loc[st, self.track[-1]])
        self._update_params_(st, self.track[-1])
        return self.track[-1], self.track[-2]

    def gen_track(self):
        for i in range(self.iters):
            self.step()
        return self.track, self.money


class Simulator(object):
    def __init__(self, initial_state_list, adj_list, state_list, fin_data_list, atrans_list, aparms_list,
                 plist_list, desired_ta, prev_story=[], iters=100, negative_event='lost', positive_event='checkout'):
        self.initial_state_list = initial_state_list
        self.adj_list = adj_list
        self.state_list = state_list
        self.fin_data_list = fin_data_list
        self.iters = iters
        self.prev_story = prev_story
        self.atrans_list, self.aparms_list, self.plist_list = atrans_list, aparms_list, plist_list
        self.agents = self._create_population_()
        self.desired_ta = desired_ta
        self.positive_event = positive_event
        self.negative_event = negative_event

    def _create_population_(self):
        agents = []
        for i in range(len(self.initial_state_list)):
            initial_state = self.initial_state_list[i]
            adj = self.adj_list[i]
            states_list = self.state_list[i]
            finance_data = self.fin_data_list[i]
            atrans = self.atrans_list[i]
            aparams = self.aparms_list[i]
            plist = self.plist_list[i]
            for key, val in initial_state.items():
                for j in range(val):
                    agents.append(Agent(key, adj, states_list, finance_data, atrans,
                                        aparams, plist, 100, atype=i,
                                        negative_event=self.negative_event, positive_event=self.positive_event))
        return agents

    def add_agents(self, cntlist, nstep):
        for i in range(len(cntlist)):
            states_list = self.state_list[i]
            finance_data = self.fin_data_list[i]
            adj = self.adj_list[i]
            atrans = self.atrans_list[i]
            aparams = self.aparms_list[i]
            plist = self.plist_list[i]
            for j in range(cntlist[i]):
                self.agents.append(
                    Agent('ta', adj, states_list, finance_data, atrans, aparams, plist, 100, atype=i, pad_len=nstep)
                )

    def __cleaner__(self):
        cnt = {}
        bads = []
        for idx, ag in enumerate(self.agents):
            if cnt.get(ag.atype) is None:
                cnt[ag.atype] = 0
            if (ag.track[-1] == 'ta') and (cnt[ag.atype] == self.desired_ta[ag.atype]):
                print(idx)
                bads.append(idx)
            elif (ag.track[-1] == 'ta'):
                cnt[ag.atype] += 1
        for i in reversed(bads):
            self.agents.pop(i)
        return cnt

    def _step_(self, nstep):
        for ag in self.agents:
            res = ag.step()

    def extract_money(self):
        m = []
        for ag in self.agents:
            m.append(ag.money)
        return np.array(m)

    def _simulation(self):
        res = []
        for i in trange(self.iters):
            self._step_(i)
            cntlist = self.__cleaner__()
            cntlist = {i: self.desired_ta[i] - cntlist[i] for i in cntlist}
            self.add_agents(cntlist, i)
            res.append(self.calc_states())
        m = self.extract_money()
        self.res = res
        return m.sum(0)

    def calc_states(self):
        res = []
        for i in self.agents:
            res.append(i.track[-1])
        return Counter(res)

    def plot_users_cumsum(self):
        x = pd.DataFrame(self.res).fillna(0)
        for i in x.columns:
            if i in [self.negative_event, self.positive_event, 'ta']: continue
            x[i] = x[i].cumsum()
        x.plot()
        return x

    def plot_users(self):
        x = pd.DataFrame(self.res).fillna(0)
        x.plot()
        return x

    def return_simulation(self):
        return self._simulation()

    def plot_simulation(self, m=None):
        if m is None:
            m = self._simulation()

        plt.rcParams['figure.figsize'] = [16, 5]
        fig, axs = plt.subplots(1, 2)

        axs[0].plot(m)
        axs[0].set_title('Money increment (difference A-B)')
        axs[1].plot(m.cumsum())
        axs[1].set_title('Money cumulative (difference A-B)')
        plt.show()
