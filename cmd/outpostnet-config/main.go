// outpostnet-config: OutpostNet配置生成和管理工具
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/generator"
)

const (
	Version = "0.1.0"
)

func main() {
	var (
		version    = flag.Bool("version", false, "Show version information")
		configFile = flag.String("config", "", "Configuration file path")
		output     = flag.String("output", "", "Output directory for generated configs")
		template   = flag.String("template", "", "Template name (express, region-mesh, production)")
		interactive = flag.Bool("interactive", false, "Interactive configuration wizard")
		validate   = flag.Bool("validate", false, "Validate configuration file")
		generate   = flag.Bool("generate", false, "Generate configuration files")
		help       = flag.Bool("help", false, "Show help information")
	)
	flag.Parse()

	if *version {
		fmt.Printf("outpostnet-config version %s\n", Version)
		return
	}

	if *help {
		showHelp()
		return
	}

	// 创建配置生成器
	configGen := generator.NewConfigGenerator()

	switch {
	case *interactive:
		// 交互式配置向导
		if err := runInteractiveWizard(configGen, *output); err != nil {
			log.Fatalf("Interactive wizard failed: %v", err)
		}

	case *validate:
		// 验证配置文件
		if *configFile == "" {
			log.Fatal("Configuration file is required for validation")
		}
		if err := validateConfig(*configFile); err != nil {
			log.Fatalf("Configuration validation failed: %v", err)
		}
		fmt.Printf("Configuration file %s is valid\n", *configFile)

	case *generate:
		// 生成配置文件
		if *configFile == "" {
			log.Fatal("Configuration file is required for generation")
		}
		if *output == "" {
			log.Fatal("Output directory is required for generation")
		}
		if err := generateConfigs(*configFile, *output, configGen); err != nil {
			log.Fatalf("Configuration generation failed: %v", err)
		}
		fmt.Printf("Configuration files generated in %s\n", *output)

	case *template != "":
		// 根据模板生成配置
		if *output == "" {
			log.Fatal("Output directory is required for template generation")
		}
		if err := generateFromTemplate(*template, *output, configGen); err != nil {
			log.Fatalf("Template generation failed: %v", err)
		}
		fmt.Printf("Template %s generated in %s\n", *template, *output)

	default:
		showHelp()
	}
}

func showHelp() {
	fmt.Print(`outpostnet-config - OutpostNet配置生成和管理工具

Usage:
  outpostnet-config [flags]

Examples:
  # 交互式配置向导
  outpostnet-config --interactive --output ./configs

  # 验证配置文件
  outpostnet-config --validate --config deployment.yaml

  # 生成配置文件
  outpostnet-config --generate --config deployment.yaml --output ./configs

  # 使用模板生成配置
  outpostnet-config --template express --output ./configs
  outpostnet-config --template region-mesh --output ./configs

Flags:
  --version            Show version information
  --interactive        Interactive configuration wizard
  --validate           Validate configuration file
  --generate           Generate configuration files
  --config string      Configuration file path
  --output string      Output directory for generated configs
  --template string    Template name (express, region-mesh, production)
  --help               Show help information

Templates:
  express              Express Mode - 快速开始配置
  region-mesh          Region Mesh - 区域服务网格配置
  production           Production - 生产环境完整配置

Configuration file format:
  deployment.yaml contains deployment specification including:
  - Cluster information
  - Service definitions
  - Network topology
  - Security settings
`)
}

func runInteractiveWizard(gen *generator.ConfigGenerator, outputDir string) error {
	fmt.Println("🚀 OutpostNet Interactive Configuration Wizard")
	fmt.Println("==========================================")

	wizard := generator.NewInteractiveWizard(gen)
	return wizard.Run(outputDir)
}

func validateConfig(configFile string) error {
	validator := config.NewValidator()
	return validator.ValidateFile(configFile)
}

func generateConfigs(configFile, outputDir string, gen *generator.ConfigGenerator) error {
	// 加载配置文件
	deployment, err := config.LoadDeployment(configFile)
	if err != nil {
		return fmt.Errorf("failed to load deployment config: %w", err)
	}

	// 生成配置文件
	return gen.Generate(deployment, outputDir)
}

func generateFromTemplate(templateName, outputDir string, gen *generator.ConfigGenerator) error {
	// 根据模板生成配置
	return gen.GenerateFromTemplate(templateName, outputDir)
}