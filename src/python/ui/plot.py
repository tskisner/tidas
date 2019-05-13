



        self.plt = pg.PlotWidget(title="Multiple curves")
        self.plt.plot(np.random.normal(size=100), pen=(255,0,0), name="Red curve")
        self.plt.plot(np.random.normal(size=110)+5, pen=(0,255,0), name="Green curve")
        self.plt.plot(np.random.normal(size=120)+10, pen=(0,0,255), name="Blue curve")
