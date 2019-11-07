"""
模拟tick挂单
"""

import copy
import time
import numpy as np
import pandas as pd


class DataCenter:

    def __init__(self,
                 init_price: float = 100.000):
        """
        初始化
        :param init_price: （发行）开盘价
        """
        self.init_price = init_price
        # 历史成交
        self.history_data = pd.DataFrame(columns=["timestamp", "price", "quantity", "direction"])
        # 尚未成交
        self.sell_data = pd.DataFrame(columns=["timestamp", "price", "quantity", "direction"])
        self.buy_data = pd.DataFrame(columns=["timestamp", "price", "quantity", "direction"])
        # 新增数据
        self.new_data = {}

    def generate_tick_data(self):
        """
        模拟一个 tick 盘口挂单数据更新
        :return: None
        """
        tmp_price = np.random.randn() // 0.01 / 100
        self.new_data = {
            "timestamp": time.time(),
            "price": self.init_price + np.abs(tmp_price),
            "quantity": np.random.randint(1, 10) * 100,
            "direction": np.sign(tmp_price),
        }

    @staticmethod
    def _append(existing_data, new_data, ascending):
        if ascending is None:
            return existing_data.append(pd.DataFrame({k: [new_data[k]] for k in new_data})).reset_index(drop=True)
        else:
            return existing_data.append(pd.DataFrame({k: [new_data[k]] for k in new_data})).sort_values(
                by="price", ascending=ascending).reset_index(drop=True)

    def match_tick_data(self):
        if self.new_data["direction"] == 1:  # 买方挂单
            available_data = self.sell_data[self.sell_data["price"] < self.new_data["price"]]
            if len(available_data) == 0:  # 卖方对手盘挂单价格过高，无法成交
                self.buy_data = self._append(self.buy_data, self.new_data, ascending=False)
            else:
                for idx in available_data.index.values[::-1]:
                    if self.new_data["quantity"] > 0:
                        record_data = copy.deepcopy(self.new_data)
                        record_data["quantity"] = min(self.new_data["quantity"], available_data.loc[idx, "quantity"])
                        if self.new_data["quantity"] < available_data.loc[idx, "quantity"]:  # 卖方数量足够
                            self.sell_data.loc[idx, "quantity"] -= self.new_data["quantity"]  # 卖方挂单减去相应数量
                        else:  # 卖方数量不足
                            self.new_data["quantity"] -= available_data.loc[idx, "quantity"]  # 买方挂单减去相应数量
                            self.sell_data.drop(index=idx, axis=0, inplace=True)  # 删除卖方挂单
                        print(record_data)
                        self.history_data = self._append(self.history_data, record_data, ascending=None)  # 成交记录进入历史成交
                if self.new_data["quantity"] > 0:
                    self.buy_data = self._append(self.buy_data, self.new_data, ascending=False)  # 买方挂单进入买盘
        else:  # 卖方挂单
            available_data = self.buy_data[self.buy_data["price"] > self.new_data["price"]]
            if len(available_data) == 0:  # 买方对手盘挂单价格过低，无法成交
                self.sell_data = self._append(self.sell_data, self.new_data, ascending=True)
            else:
                for idx in available_data.index.values:
                    if self.new_data["quantity"] > 0:
                        record_data = copy.deepcopy(self.new_data)
                        record_data["quantity"] = min(self.new_data["quantity"], available_data.loc[idx, "quantity"])
                        if self.new_data["quantity"] < available_data.loc[idx, "quantity"]:  # 买方数量足够
                            self.buy_data.loc[idx, "quantity"] -= self.new_data["quantity"]  # 买方挂单减去相应数量
                        else:  # 买方数量不足
                            self.new_data["quantity"] -= available_data.loc[idx, "quantity"]  # 卖方挂单减去相应数量
                            self.buy_data.drop(index=idx, axis=0, inplace=True)  # 删除卖方挂单
                        print(record_data)
                        self.history_data = self._append(self.history_data, record_data, ascending=None)  # 成交记录进入历史成交
                if self.new_data["quantity"] > 0:
                    self.sell_data = self._append(self.sell_data, self.new_data, ascending=True)  # 卖方挂单进入买盘


if __name__ == "__main__":
    data_center = DataCenter(init_price=100.00)
    cnt = 0
    while cnt < 10:
        data_center.generate_tick_data()
        data_center.match_tick_data()
        cnt += 1
        time.sleep(0.1)
