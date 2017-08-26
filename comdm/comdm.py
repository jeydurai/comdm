import click
import os
from .commands.backup import Backup
from .commands.restore import Restore
from .commands.upload import Upload
from .commands.subset import Subset
from .commands.clean import CleanBookingDump
from .commands.test import TestComDM

class Config(object):
    """Configuration class to connect group command and 
    sub commands"""

    def __init__(self):
        self.dbtype = 'mongodb'


pass_config = click.make_pass_decorator(Config, ensure=True)


@click.group()
@click.option('--dbtype', default='mongodb', help="Type of database")
@click.option('--home-dir', type=click.Path(), help="Home directory location")
@pass_config
def main(config, dbtype, home_dir):
    """Main Command to handle ccsdm commands"""
    config.dbtype = dbtype
    if home_dir is None:
        home_dir = os.path.expanduser('~')
    config.home_dir = home_dir


@main.command()
@click.option('--out', '-o', default=os.path.expanduser('~'), help='Specifies the "dump" folder path')
@pass_config
def backup(config, out):
    """Handle all databases' backup"""
    click.echo('Backup command runs')
    bkup = Backup(out)
    bkup.run()
    click.echo('"backup" command finished running')

@main.command()
@click.option('--filepath', '-f', default=os.path.expanduser('~'),
              help='Absolute path of the dump folder to be restored')
@pass_config
def restore(config, filepath):
    """Handle all databases' restoration"""
    click.echo('Restore command runs')
    rstr = Restore(filepath + '\\' + 'dump')
    rstr.run()
    click.echo('"restore" command finished running')


@main.command()
@click.argument('source')
@click.argument('desti')
@click.option('--sheetname', '-s', help="Sheet name of the excel sheet to be read")
@pass_config
def upload(config, source, desti, sheetname):
    """Upload Excel files into MongoDB database"""
    upld = Upload(config.home_dir, source, desti, sheetname)
    (is_valid, msg) = upld.validate()
    click.echo(msg)
    if is_valid:
        click.echo('Execute "upload"...')
        upld.read_xl_upload_mongo()

    
@main.command()
@click.option('--sl3', help="Takes sales_level_3")
@click.option('--sl4', help="Takes sales_level_4")
@click.option('--sl5', help="Takes sales_level_5")
@click.option('--sl6', help="Takes sales_level_6")
@click.option('--salesagent', help="Takes sales_agent")
@click.option('--year','-y',  help="Takes Financial Year")
@click.option('--quarter','-q',  help="Takes Financial Quarter")
@click.option('--month','-m',  help="Takes Financial Month")
@click.option('--week','-w',  help="Takes Financial Week")
@click.option('--servindi', type=click.Choice(['Y', 'N']), help="N - Products and Y - Services")
@click.option('--sensitivity', type=click.Choice(['1', '2']), help="Sensitive fields visibility")
@click.option('--out', help="Outfile path for saving resultant file")
@click.option('--writeas', type=click.Choice(['xlsx', 'csv']), help="Write as file")
@click.option('--verbose/--no-verbose', default=False, help="More verbose output on STDOUT")
@click.option('--cloud/--no-cloud', default=True, help="Switch to enable Cloud booking")
@click.option('-xc', '--excludecus', multiple=True, help="Excludes the arrays of customers")
@click.option('--onlycom/--no-onlycom', default=True, help="Always qualifies SL3 as 'Commercial'")
@pass_config
def subset(config, sl3, sl4, sl5, sl6, salesagent, year, quarter, month, week, 
        servindi, sensitivity, out, writeas, verbose, cloud, excludecus, 
        onlycom):
    """Subsets from MongoDB & writes as an excel file"""
    options = {
        'config': config,
        'sl3': sl3,
        'sl4': sl4,
        'sl5': sl5,
        'sl6': sl6,
        'salesagent': salesagent,
        'year': year,
        'quarter': quarter,
        'month': month,
        'week': week,
        'servindi': servindi,
        'sensitivity': sensitivity,
        'out': out,
        'writeas': writeas,
        'verbose': verbose,
        'cloud': cloud,
        'excludecus': excludecus,
        'onlycom': onlycom,
    }
    ss = Subset(options)
    ss.process()
    click.echo('Report Finished!')


@main.command()
@click.option('--sl3', help="Takes sales_level_3")
@click.option('--sl4', help="Takes sales_level_4")
@click.option('--sl5', help="Takes sales_level_5")
@click.option('--sl6', help="Takes sales_level_6")
@click.option('--salesagent', help="Takes sales_agent")
@click.option('--years','-y',  help="No/. years to be processed")
@click.option('--servindi', type=click.Choice(['Y', 'N']), help="N - Products and Y - Services")
@click.option('--sensitivity', type=click.Choice(['1', '2']), help="Sensitive fields visibility")
@click.option('--out', help="Outfile path for saving resultant file")
@click.option('--writeas', type=click.Choice(['xlsx', 'csv', 'pkl']), help="Write as file")
@click.option('--writeable', type=click.Choice(['all', 'noncloud', 'products', 'services']),
              help="Write as file")
@click.option('--verbose/--no-verbose', default=False, help="More verbose output on STDOUT")
@click.option('--cloud/--no-cloud', default=True, help="Switch to enable Cloud booking")
@click.option('-xc', '--excludecus', multiple=True, help="Excludes the arrays of customers")
@click.option('--comparefull/--no-comparefull', default=True, help="Compare full year?")
@click.option('--unamemap/--no-unamemap', default=True, help="Should map uniquenames?")
@click.option('--ammap/--no-ammap', default=True, help="Should map AMs?")
@pass_config
def cleanbd(config, sl3, sl4, sl5, sl6, salesagent, years, servindi, sensitivity, out,
            writeas, writeable, verbose, cloud, excludecus, comparefull, unamemap,
            ammap):
    """Cleanses the ent_dump_from_finance data for any subset"""
    options = {
        'config': config,
        'sl3': sl3,
        'sl4': sl4,
        'sl5': sl5,
        'sl6': sl6,
        'salesagent': salesagent,
        'years': years,
        'servindi': servindi,
        'sensitivity': sensitivity,
        'out': out,
        'writeas': writeas,
        'writeable': writeable,
        'verbose': verbose,
        'cloud': cloud,
        'excludecus': excludecus,
        'comparefull': comparefull,
        'unamemap': unamemap,
        'ammap': ammap,
    }
    bd = CleanBookingDump(options)
    bd.process()
    click.echo('Dump Extraction completed!')

@main.command()
@pass_config
def mytest(config):
    """For temporary testing"""
    click.echo('Testing initiated')
    test = TestComDM()
    test.run()
    click.echo('Testing Ended!')

    
