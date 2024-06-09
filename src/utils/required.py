import click

class NotRequiredIf(click.Option):
    def __init__(self, *args, **kwargs):
        super(NotRequiredIf, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        exists = self.name in opts
     
        if not exists:
            raise click.UsageError(
                "Illegal usage: `%s` is Required" % (self.name))
        else:
                self.prompt = None

        return super(NotRequiredIf, self).handle_parse_result(
            ctx, opts, args)