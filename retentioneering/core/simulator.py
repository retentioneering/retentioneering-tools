import numpy as np
import os
import pandas as pd


class Agent:
    def __init__(self, probs, user_type, config=None, user_id=None):
        self._mean_time = probs.loc['dt_mean']
        self._probs = probs.drop(['dt_mean'])
        self._probs = self._probs / self._probs.sum(0)
        self._state = 1
        self.user_type = user_type
        if user_id is None:
            user_id = np.random.randint(1e6)
        self.user_id = user_id
        if config is None:
            self._track = pd.DataFrame(columns=['user_pseudo_id', 'event_timestamp', 'event_name', 'user_type'])
        else:
            self._track = pd.DataFrame(columns=[config['index_col'], config['event_time_col'], config['event_col'], 'user_type'])
        self._current_time = pd.datetime.now().timestamp() * 1e9
        self.event=''
        self.config = config
        
    def _step(self):
        probs = self._probs.loc[:, str(self._state)]
        vals = probs.index
        probs = np.nan_to_num(probs.values)
        if np.sum(probs) != 0:
            return np.random.choice(vals, p=probs)
        return 

    def _delay(self):
        mu = self._mean_time.loc[str(self._state)]
        time = np.random.exponential(scale=mu,size=1)[0]
        return time
        
    def simulate(self):
        while (self._state != -1) and (self._state < self._probs.shape[1]):
            self._current_time += self._delay() * 10**12
            self.event = self._step()
            if self.event is None:
                break
            elif self.event == self.config['negative_target_event']:
                self._state = -1
            else:
                self._state += 1
            self._track = self._track.append({
                self.config['index_col']: self.user_id,
                self.config['event_time_col']: self._current_time,
                self.config['event_col']: self.event,
                'user_type': self.user_type
            }, ignore_index=True)
        return self._track



class Simulator:
    def __init__(self, path='', tables='dynamic_matrix', num_pops=100):
        """
        :param path: path to the base folder
        :param tables: str of the folder with tables of a list with step matrices (get_step_matrix in utils)
        :param num_pops: list with the number of agents for each cluster or 100 for everyone
        :param means: a list of lists of dt_means for tables, if None then 0.8 for every step
        """
        config = pd.DataFrame().retention.retention_config
        self._population = self.create_population(path, config, tables, num_pops)
        #self.clickstream = pd.DataFrame(columns=['user_pseudo_id', 'event_timestamp', 'event_name', 'user_type'])
        self.clickstream = pd.DataFrame(columns=[config['index_col'], config['event_time_col'], config['event_col'], 'user_type'])
        
    @staticmethod    
    def prepare_step_matrix(mat):
        """
        :param mat: step matrix with dt_means
        :return: prepared matrix
        """
        config = mat.retention.retention_config
        mat = mat.drop(index=['Accumulated {}'.format(config.get('negative_target_event')), 'Accumulated {}'.format(config.get('positive_target_event'))])
        mat.columns = mat.columns.astype(str)
        return mat
        
    # this->refactor
    def create_population(self, path, config, tables, num_pops):
        if type(tables) == str:  # through folders
            dyn_mat = os.path.join(path, tables)
            files = sorted(list(filter(lambda x : x[0]!='.', os.listdir(dyn_mat))))  # collect all files except MacOS system files
        else:  # if tables is a list
            files = tables
        agents = []
        for idx, file in enumerate(files):
            if type(num_pops) == int:
                agents_num = num_pops  #int(pd.read_csv(os.path.join(stats, file)).users_count.iloc[0])
            else:
                agents_num = num_pops[idx]
            if type(tables) == str:
                clus_dyn = pd.read_csv(os.path.join(dyn_mat, file), index_col=[0])
                for i in range(agents_num):
                    agents.append(Agent(clus_dyn, file.split('_')[1].split('.')[0], config))
            else:
                step_matr = self.prepare_step_matrix(file)
                for i in range(agents_num):
                    agents.append(Agent(step_matr, idx, config))
        return agents

    def simulate(self):
        for agent in self._population:
            self.clickstream = self.clickstream.append(agent.simulate())
        return self.clickstream