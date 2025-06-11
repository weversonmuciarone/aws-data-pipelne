import yaml


# class to read variables from variable.yaml
class VariableReader:

    def __init__(self, path="variables.yaml", config: str = None):
        if config is None and path is None:
            raise Exception("Expecting variables or path")
        self.config = config
        self.path = path
        self.definition = self.parse()

    def parse(self):
        if not self.config:
            with open(self.path, "r") as f:
                definition = yaml.safe_load(f)
        else:
            definition = yaml.safe_load(self.config)

        return definition
